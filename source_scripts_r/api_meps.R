###--------------------------------------------------------------------------###
# MEPs Open Data API -----------------------------------------------------------
###--------------------------------------------------------------------------###

# rm(list = ls())

#' This script connects to the EP Open Data Portal API to download the list of MEPs for the current mandate (i.e. 9th).
#' The script implies that `ep_rcv_mandate.R` has already been executed.
#' This is because it needs the output `.csv` from that script to extract the Plenary dates.
#' There seems to be an issue with `GUE`- `The Left`, as MEPs belonging to that Group are reported twice (as the Group changed name during the mandate).
#' This needs to be sorted downstream.
#' REF: https://data.europarl.europa.eu/en/home; https://data.europarl.europa.eu/en/developer-corner/opendata-api

###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
library(tidyr)
library(dplyr)
library(lubridate)
library(data.table)
library(httr)
library(jsonlite)
library(future.apply)


###--------------------------------------------------------------------------###
## GET/meps --------------------------------------------------------------------
# Returns the list of all the MEPs --------------------------------------------#

#' We start by collecting the list of MEPs for the 9th mandate.

# API call --------------------------------------------------------------------#
# EXAMPLE: https://data.europarl.europa.eu/api/v2/meps?parliamentary-term=9&format=application%2Fld%2Bjson&offset=0&limit=50
api_url <- "https://data.europarl.europa.eu/api/v2/meps?parliamentary-term=9&format=application%2Fld%2Bjson&offset=0"
api_raw <- httr::GET(api_url)
api_list <- jsonlite::fromJSON(
  rawToChar(api_raw$content), flatten = TRUE)
meps_mandate <- api_list$data |>
  janitor::clean_names() |>
  dplyr::select(mep_name = label,
                pers_id = identifier) |>
  dplyr::mutate(pers_id = as.integer(pers_id))

# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = meps_mandate,
                   file = here::here("data_out", "meps_mandate.csv") )

# Remove API objects --------------------------------------------------------###
rm(api_raw, api_url, api_list)

###--------------------------------------------------------------------------###
## GET/MEP-ID ------------------------------------------------------------------

#' Having collected all MEPs for the current mandate, we get detailed info on all of them.

# get MEPs' IDs
mep_ids <- sort(unique(meps_mandate$pers_id))
# create grid to loop over
api_params <- paste0("https://data.europarl.europa.eu/api/v2/meps/",
                     mep_ids,
                     "?format=application%2Fld%2Bjson")

# get status code from API ----------------------------------------------------#
url_list_tmp <- lapply(
  X = setNames(object = api_params, nm = mep_ids),
  FUN = function(api_url) {
    Sys.sleep(5) # call politely
    print(api_url) # check
    # Get data from URL
    httr::GET(api_url) } )

# get data from API -----------------------------------------------------------#
get_mep_id <- function(links = url_list_tmp) {
  future.apply::future_lapply(
    X = links, FUN = function(i_url) {
      print(i_url$url) # check
      if ( httr::status_code(i_url) %in% c(200L) ) {
        # Get data from .json
        api_list <- jsonlite::fromJSON( rawToChar(i_url$content) )
        # extract info
        return(api_list$data) } } ) }

# parallelisation -------------------------------------------------------------#
future::plan(strategy = multisession) # Run in parallel on local computer
meps_ids_list <- get_mep_id()
future::plan(strategy = sequential) # revert to normal


###--------------------------------------------------------------------------###
### Get mandate, political group, and national party information ---------------
hasMembership <- lapply(
  X = meps_ids_list, FUN = function(i_data) {
    i_data |>
      dplyr::select(pers_id = identifier,
                    hasMembership) |>
      tidyr::unnest(hasMembership, keep_empty = TRUE) |>
      tidyr::unnest_wider(memberDuring, names_sep = "_") } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::select( -dplyr::any_of( "contactPoint" ) ) |>
  dplyr::mutate(pers_id = as.integer(pers_id)) |>
  janitor::clean_names()
# unique(hasMembership$membership_classification)

# Create grid of MEPs and Dates -----------------------------------------------#
# Read in data
if ( !exists("votes_dt") ) {
  votes_dt <- data.table::fread( file = here::here("data_out", "votes_dt.csv") ) }

# Grid
meps_data_grid <- expand.grid(
  activity_date = unique(votes_dt$activity_date),
  pers_id = unique(meps_mandate$pers_id) )


###--------------------------------------------------------------------------###
### Extract start and end of mandate for each MEP, as well as country ----------
meps_start_end <- hasMembership[
  grepl("MEMBER_PARLIAMENT", x = role, ignore.case = TRUE)
  & organization == "org/ep-9",
  list(pers_id,
       start_date = member_during_start_date,
       end_date = member_during_end_date)] |>
  dplyr::mutate(
    start_date = lubridate::as_date(start_date), # tz = "Europe/Brussels"
    end_date = ifelse(is.na(end_date), as.character(Sys.Date()), end_date),
    end_date = lubridate::as_date(end_date) ) |> # tz = "Europe/Brussels"
  dplyr::left_join(
    y = hasMembership[
      !is.na(represents)
      & organization == "org/ep-9",
      list(pers_id, represents)],
    by = "pers_id") |>
  dplyr::mutate(represents := gsub(
    pattern = "http://publications.europa.eu/resource/authority/country/",
    replacement = "",
    x = represents) )

# Merge actual MEPs' periods with full grid of dates
meps_dates <- merge(x = meps_data_grid, y = meps_start_end,
                    by = "pers_id", all = TRUE) |>
  data.table::as.data.table()

# Filter for just TRUE dates
meps_dates[, `:=`(
  to_keep = ifelse(
    test = activity_date >= start_date & activity_date <= end_date,
    yes = 1L, no = 0L)),
  by = list(pers_id)]
meps_dates <- meps_dates[to_keep == 1L]
meps_dates[, c("to_keep", "start_date", "end_date") := NULL]


###--------------------------------------------------------------------------###
### Extract political groups ---------------------------------------------------
meps_polgroups <- hasMembership[
  grepl("EU_POLITICAL_GROUP", x = membership_classification, ignore.case = TRUE),
  list(pers_id,
       polgroup_id = organization,
       start_date = member_during_start_date,
       end_date = member_during_end_date)] |>
  dplyr::mutate(
    polgroup_id = as.integer( gsub(pattern = "org/", replacement = "", x = polgroup_id) ),
    start_date = lubridate::as_date(start_date),
    end_date = ifelse(is.na(end_date), as.character(Sys.Date()), end_date),
    end_date = lubridate::as_date(end_date)) |>
  dplyr::filter(end_date > as.Date("2019-06-30"))

# Merge
meps_dates_polgroups <- merge(x = meps_dates,
                              y = meps_polgroups,
                              by = "pers_id", all = TRUE, allow.cartesian = TRUE) |>
  data.table::as.data.table()

# Filter for just TRUE dates
meps_dates_polgroups[, `:=`(
  to_keep = ifelse(
    test = activity_date >= start_date & activity_date <= end_date,
    yes = 1L, no = 0L)),
  by = list(pers_id)]
meps_dates_polgroups <- meps_dates_polgroups[to_keep == 1L]
meps_dates_polgroups[, c("to_keep", "represents", "start_date", "end_date") := NULL]


###--------------------------------------------------------------------------###
### Extract national parties groups --------------------------------------------
meps_natparties <- hasMembership[
  grepl("NATIONAL_CHAMBER", x = membership_classification, ignore.case = TRUE),
  list(pers_id, natparty_id = organization,
       start_date = member_during_start_date, end_date = member_during_end_date)] |>
  dplyr::mutate(
    natparty_id = as.integer( gsub(pattern = "org/", replacement = "", x = natparty_id) ),
    start_date = lubridate::as_date(start_date),
    end_date = ifelse(is.na(end_date), as.character(Sys.Date()), end_date),
    end_date = lubridate::as_date(end_date)) |>
  dplyr::filter(end_date > as.Date("2019-06-30"))

# Merge
meps_dates_natparties <- merge(x = meps_dates,
                               y = meps_natparties,
                               by = "pers_id", all = TRUE, allow.cartesian = TRUE) |>
  data.table::as.data.table()

# Filter for just TRUE dates
meps_dates_natparties[, `:=`(
  to_keep = ifelse(
    test = activity_date >= start_date & activity_date <= end_date,
    yes = 1L, no = 0L)),
  by = list(pers_id)]
meps_dates_natparties <- meps_dates_natparties[to_keep == 1L]
meps_dates_natparties[, c("to_keep", "represents", "start_date", "end_date") := NULL]


###--------------------------------------------------------------------------###
### Merge MEPs' MANDATES, Political Groups, and National Parties ---------------
meps_dates_ids <- merge(meps_dates, meps_dates_polgroups,
                        by = c("pers_id", "activity_date"), all = TRUE)
meps_dates_ids <- merge(meps_dates_ids, meps_dates_natparties,
                        by = c("pers_id", "activity_date"), all = TRUE)

# Fix data entry issues -------------------------------------------------------#
# sapply(meps_dates_ids, function(x) sum(is.na(x)))
# https://www.europarl.europa.eu/meps/en/185974/JORDI_SOLE/history/9#detailedcardmep
meps_dates_ids[pers_id == 185974L & is.na(polgroup_id),
               polgroup_id := 5152L]

# rename col ------------------------------------------------------------------#
data.table::setnames(meps_dates_ids, old = c("represents"), new = c("country"))

# Get Country dictionary -------------------------------------------------------
country_dict <- data.frame(
  country = sort(unique(meps_dates_ids$country)),
  country_id = seq_along(unique(meps_dates_ids$country) ) )

# merge -----------------------------------------------------------------------#
meps_dates_ids <- merge(x = meps_dates_ids, y = country_dt,
      by = "country", all = TRUE)
# delete col
meps_dates_ids[, country := NULL]

# write to disk ---------------------------------------------------------------#
data.table::fwrite(x =  meps_dates_ids,
                   file = here::here("data_out", "meps_dates_ids.csv"))
data.table::fwrite(x =  country_dict,
                   file = here::here("data_out", "country_dict.csv"))

# remove intermediate objects
rm(hasMembership, meps_data_grid, meps_dates, meps_mandate, meps_start_end, meps_ids_list,
   meps_polgroups, meps_dates_polgroups,
   meps_natparties, meps_dates_natparties)

# test:before brexit, N should be 751; after brexit, 705
# meps_dates_ids[, .N, keyby = list(activity_date)]
