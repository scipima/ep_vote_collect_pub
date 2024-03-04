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
library(tidyverse)
library(data.table)
library(httr)
library(jsonlite)
library(future.apply)


###--------------------------------------------------------------------------###
## GET/meps --------------------------------------------------------------------
# Returns the list of all the MEPs --------------------------------------------#
api_raw <- httr::GET(
  url = "https://data.europarl.europa.eu/api/v1/meps?parliamentary-term=9&format=application%2Fld%2Bjson&offset=0")
api_list <- jsonlite::fromJSON(
  rawToChar(api_raw$content),
  flatten = TRUE)
meps_mandate <- api_list$data |>
  janitor::clean_names() |>
  dplyr::select(mep_name = label, 
                pers_id = identifier)

# Remove API objects --------------------------------------------------------###
rm(api_raw, api_list)

###--------------------------------------------------------------------------###
## GET/MEP-ID ------------------------------------------------------------------

#' Having collected all MEPs for the current mandate, we get detailed info on all of them.

### Loop -----------------------------------------------------------------------
# we subset to just the JSON that are currently populated
mep_ids <- unique(meps_mandate$pers_id)
# grid to loop over
api_base <- "https://data.europarl.europa.eu/api/v1"
api_params <- paste0(api_base, "/meps/", mep_ids,
                     "?format=application%2Fld%2Bjson&json-layout=framed")

# Function ------------------------------------------------------------------###
get_mep_id <- function(links = api_params) {
  future.apply::future_lapply(
    X = links, FUN = function(param) {
      print(param) # check
      api_url <- param
      api_raw <- httr::GET(api_url)
      api_list <- jsonlite::fromJSON(
        rawToChar(api_raw$content),
        flatten = TRUE)
      return( api_list$data ) } ) }

# perform parallelised check
future::plan(strategy = multisession) # Run in parallel on local computer
list_tmp <- get_mep_id()
future::plan(strategy = sequential) # revert to normal


###--------------------------------------------------------------------------###
### Get mandate, political group, and national party information --------------- 
hasMembership <- lapply(
  X = list_tmp, 
  FUN = function(i_data) {
    i_data |>
      dplyr::select(pers_id = identifier,
                    hasMembership) |> 
      tidyr::unnest(hasMembership) } ) |> 
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |> 
  dplyr::select(-contactPoint)

# Create grid of MEPs and Dates -----------------------------------------------#
# Read in data
votes_dt <- data.table::fread( file = here::here("data_out", "votes_dt.csv") )
# Grid
meps_data_grid <- expand.grid(
  activity_date = unique(votes_dt$activity_date),
  pers_id = unique(meps_mandate$pers_id) )


###--------------------------------------------------------------------------###
### Extract start and end of mandate for each MEP, as well as country ----------
meps_start_end <- hasMembership[
  grepl("member_ep", x = role, ignore.case = TRUE)
  & organization == "org/ep-9",
  list(pers_id, start_date = memberDuring.startDate, end_date = memberDuring.endDate)] |> 
  dplyr::mutate(
    start_date = lubridate::as_date(start_date),
    end_date = ifelse(is.na(end_date), as.character(Sys.Date()), end_date),
    end_date = lubridate::as_date(end_date)) |> 
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

# Merge
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
  grepl("ep_group", x = membershipClassification, ignore.case = TRUE),
  list(pers_id, polgroup_id = organization, 
       start_date = memberDuring.startDate, end_date = memberDuring.endDate)] |> 
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
meps_dates_polgroups[, c("to_keep", "start_date", "end_date") := NULL]


###--------------------------------------------------------------------------###
### Extract national parties groups --------------------------------------------
meps_natparties <- hasMembership[
  grepl("np", x = membershipClassification, ignore.case = TRUE),
  list(pers_id, natparty_id = organization, 
       start_date = memberDuring.startDate, end_date = memberDuring.endDate)] |> 
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
meps_dates_natparties[, c("to_keep", "start_date", "end_date") := NULL]


###--------------------------------------------------------------------------###
### Merge MEPs' MANDATES, Political Groups, and National Parties ---------------
meps_dates_ids <- merge(meps_dates, meps_dates_polgroups, 
                        by = c("pers_id", "activity_date"), all = TRUE)
meps_dates_ids <- merge(meps_dates_ids, meps_dates_natparties, 
                        by = c("pers_id", "activity_date"), all = TRUE)

# write to disk ---------------------------------------------------------------#
data.table::fwrite(x =  meps_dates_ids, 
                   file = here::here("data_out", "meps_dates_ids.csv"))

# remove intermediate objects
rm(hasMembership, meps_data_grid, meps_dates, meps_mandate, meps_start_end,
   meps_polgroups, meps_dates_polgroups,
   meps_natparties, meps_dates_natparties)

















###--------------------------------------------------------------------------###
## Merge MEPs and BODIES -------------------------------------------------------

###--------------------------------------------------------------------------###
#' Merge the full MEPs' history with the details of bodies.
#' In this way, we get information on parties, EP political groups, etc, which were missing before.
###--------------------------------------------------------------------------###

# merge
meps_fullhistory_long <- merge(
  x = mep_id_full, y = body_id_full,
  by.x = "organization_id", by.y = "identifier",
  all.x = FALSE, all.y = TRUE)
# drop cols
meps_fullhistory_long <- meps_fullhistory_long[, list(
  country, family_name, sort_label, mep_name = label.x, mep_id = identifier,
  organization_id, classification, body_label = label.y, body_name_en = alt_label_en,
  body_fullname_en = pref_label_en, role, member_during_start_date,
  member_during_end_date)]
# clean cols
meps_fullhistory_long[, `:=`(
  role = gsub(
    pattern = "http://publications.europa.eu/resource/authority/role/",
    replacement = "", x = role),
  classification = gsub(
    pattern = "http://publications.europa.eu/resource/authority/corporate-body-classification/",
    replacement = "", x = classification) ) ]
# write data
data.table::fwrite(x = meps_fullhistory_long,
                   here::here("data_out", "meps", "meps_fullhistory_long.csv"))

###--------------------------------------------------------------------------###
#' For our purposes, the disadvantage of `meps_fullhistory_long` is that we also have change of status within bodies (this is recorded as `role`).
#' For instance, a change from `Member` to `Chair` is recorded there, resulting in multiple rows.
#' Therefore we summarise the dataset by taking the first and last timestamps for each MEP and within body, so that we get a continuous time period for each membership.
###--------------------------------------------------------------------------###

meps_history_long <- meps_fullhistory_long[
  grepl(pattern = "EP_GROUP|NP", x = classification),]
# overwrite change in POLITICAL GROUP: "The Left"
meps_history_long[organization_id == 5151, `:=`(
  organization_id = 6259,
  body_label = "The Left",
  body_name_en = "The Left - GUE/NGL",
  body_fullname_en = "The Left group in the European Parliament - GUE/NGL")]
# overwrite same national party, but different in EP data
meps_history_long[organization_id == 5445, `:=`(
  organization_id = 5264,
  body_label = "FDI",
  body_name_en = "FDI",
  body_fullname_en = "Fratelli d'Italia")]
# sort data to get changes in chronological order, by `classification`
data.table::setorder(x = meps_history_long,
                     mep_id, classification, member_during_start_date, organization_id)
# unique(meps_history_long$role)
dim(meps_history_long)
# drop ROLE
meps_history_long <-  unique(meps_history_long[, c("role") := NULL])
dim(meps_history_long)
# subset just the first row, ordered above
meps_history_long <- meps_history_long[, head(.SD, 1L),
                                       by = list(mep_id, classification,
                                                 organization_id)]
dim(meps_history_long)
# write data
data.table::fwrite(x = meps_history_long,
                   here::here("data_out", "meps", "meps_history_long.csv"))

# Remove API objects --------------------------------------------------------###
rm(api_raw, api_list, list_tmp)

