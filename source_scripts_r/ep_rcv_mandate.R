###--------------------------------------------------------------------------###
# Daily EP Votes ---------------------------------------------------------------
###--------------------------------------------------------------------------###

# rm(list = ls())

###--------------------------------------------------------------------------###
#' DESCRIPTION.
#' We follow 4 steps to get the daily RCVs from the EP Open Data Portal (https://data.europarl.europa.eu/en/developer-corner/opendata-api).
#' First, we grab the calendar and subset it to get the identifier for the last Plenary.
#' Second, with that identifier, we grab the RCV.
#' Third, we get the list of the current MEPs, with all the details.
#' Forth, and because the current MEPs list does not contain info on national party, we need to grab a further dataset with all the MEPs info, and subset to the last membership.
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
if (!require("pacman")) install.packages("pacman")
pacman::p_load(char = c("data.table", "dplyr", "tidyr", "tidyselect", "future.apply",
                        "httr", "here", "lubridate", "janitor", "jsonlite") )

# check if dir exists to dump processed files
if ( !dir.exists(here::here("data_out") ) ) {
  dir.create(here::here("data_out") ) }


###--------------------------------------------------------------------------###
## GET Meetings ----------------------------------------------------------------
# EXAMPLE: https://data.europarl.europa.eu/api/v1/plenary-documents?year=2016&format=application%2Fld%2Bjson&offset=0
# create parameters to loop over
api_base <- "https://data.europarl.europa.eu/api/v1"
years <- 2019 : data.table::year(Sys.Date()) 

# grid to loop over
api_params <- paste0(api_base, "/meetings?year=", years,
                     "&format=application%2Fld%2Bjson&offset=0")

# get data from API -----------------------------------------------------------#
get_meetings_json <- lapply(
  X = api_params,
  FUN = function(api_url) {
    print(api_url)
    api_raw <- httr::GET(api_url)
    return(api_raw) } )
names(get_meetings_json) <- years

# get data from JSON -----------------------------------------------------------#
json_list <- lapply(
  X = get_meetings_json,
  function(i_json) {
    print(i_json$url) # check
    if ( httr::status_code(i_json) != 404 ) {
      # Get data from .json
      api_list <- jsonlite::fromJSON(
        rawToChar(i_json$content))
      # # extract info
      return(api_list$data) } 
      } )


# append data ---------------------------------------------------------------###
calendar <- data.table::rbindlist(json_list,
                                  use.names=TRUE, fill=TRUE, idcol="year")
# sapply(plenary_documents, function(x) sum(is.na(x)))
# clean data
calendar[, `:=`(year = as.integer(year),
                date = as.Date(gsub(pattern = "eli/dl/event/MTG-PL-",
                                    replacement = "", x = id)))]
# get just the Plenaries that have already taken place, including today
calendar <- calendar[date >= as.Date("2019-07-01")
                     & date <= Sys.Date()]
# get identifier for today's Plenary
calendar[, c("type", "id", "had_activity_type") := NULL]

# Remove API objects --------------------------------------------------------###
rm(api_base, api_params, get_meetings_json, json_list)


###--------------------------------------------------------------------------###
## GET/meetings/{event-id}/decisions -------------------------------------------
# Returns all decisions in a single EP Meeting --------------------------------#
# EXAMPLE: "https://data.europarl.europa.eu/api/v1/meetings/MTG-PL-2023-07-12/decisions?format=application%2Fld%2Bjson&json-layout=framed"

# get status code from API ----------------------------------------------------#
url_list_tmp <- lapply(
  X = setNames(object = calendar$activity_id,
               nm = calendar$activity_id),
  FUN = function(i_param) {
    # grid to loop over
    print(i_param) # check
    Sys.sleep(1) # call politely
    api_url <- paste0("https://data.europarl.europa.eu/api/v1/meetings/",
                      i_param,
                      "/decisions?format=application%2Fld%2Bjson&json-layout=framed")
    # Get data from URL
    httr::GET(api_url) } )

# get data from API -----------------------------------------------------------#
get_vote <- function(links = url_list_tmp) {
  future.apply::future_lapply(
    X = links, FUN = function(i_url) {
      print(i_url$url) # check
      if ( httr::status_code(i_url) != 404 ) {
        # Get data from .json
        api_list <- jsonlite::fromJSON(
          rawToChar(i_url$content) )
        # extract info
        return(api_list$data) } } ) }

# parallelisation -----------------------------------------------------------###
future::plan(strategy = multisession) # Run in parallel on local computer
vote_list_tmp <- get_vote()
future::plan(strategy = sequential) # revert to normal

# remove objects --------------------------------------------------------------#
rm(url_list_tmp)

###--------------------------------------------------------------------------###
# Source functions ------------------------------------------------------------#

#' These 2 functions clean, respectively, the vote data and the RCV data.

source(file = here::here("source_scripts_r", "process_vote_day.R"))
source(file = here::here("source_scripts_r", "process_rcv_day.R"))
###--------------------------------------------------------------------------###

# Get Votes and RCV
votes_dt <- lapply(X = vote_list_tmp, FUN = function(x) process_vote_day(x) ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE, idcol = "plenary_id")
rcv_dt <- lapply(X = vote_list_tmp, FUN = function(x) process_rcv_day(x) ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE, idcol = "plenary_id")
rcv_dt[, activity_date := as.Date(gsub(pattern = "MTG-PL-",
                                       replacement = "", x = plenary_id))]
rcv_dt[, plenary_id := NULL]
# sapply(rcv_dt, function(x) sum(is.na(x))) # check


# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = votes_dt,
                   file = here::here("data_out", "votes_dt.csv") )
data.table::fwrite(x = rcv_dt,
                   file = here::here("data_out", "rcv_dt.csv") )
                   
# remove objects --------------------------------------------------------------#                   
rm(vote_list_tmp)

###--------------------------------------------------------------------------###
# Source script ---------------------------------------------------------------#

#' Get clean data on MEPs' membership and mandate duration.

source(file = here::here("source_scripts_r", "meps_api.R"))
# sapply(meps_dates_ids, function(x) sum(is.na(x))) # check

#' Get look-up tables for MEPs' memberships, and fetch `join_functions.R`

source(file = here::here("source_scripts_r", "ep_bodies.R"))
source(file = here::here("source_scripts_r", "join_functions.R"))
###--------------------------------------------------------------------------###


###--------------------------------------------------------------------------###
## Merge RCV with MEPs ---------------------------------------------------------
# read in file if not present already
if ( !exists("meps_dates_ids") ) {
  meps_dates_ids <- data.table::fread(file = here::here(
    "data_out", "meps_dates_ids.csv") ) }

# Create a grid with all MEPs who SHOULD have been present
meps_rcv_grid <- merge(
  x = meps_dates_ids[, list(activity_date, pers_id)],
  y = unique(rcv_dt[,  list(activity_date, notation_votingId) ] ) ,
  by = "activity_date", all = TRUE, allow.cartesian = TRUE) |>
  dplyr::left_join(y = meps_dates_ids,
                   by = c("pers_id", "activity_date") )
meps_rcv_grid[, activity_date := as.Date(activity_date)]
# dim(meps_rcv_grid)
# sapply(meps_rcv_grid, function(x) sum(is.na(x)))

# merge grid with RCV data
meps_rcv_mandate <- merge(x = meps_rcv_grid, y = rcv_dt,
                          by = c("activity_date", "notation_votingId", "pers_id"),
                          all = TRUE) |>
  data.table::as.data.table()

# check
if ( nrow(meps_rcv_mandate) > nrow(meps_rcv_grid) ) {
  warning("WATCH OUT: You may have duplicate records") }


###--------------------------------------------------------------------------###
# Final cleaning --------------------------------------------------------------#
# Flag for ABSENT
meps_rcv_mandate[, is_absent := mean( is.na(result)),
                 by = list(activity_date, pers_id)]
meps_rcv_mandate[, is_absent := data.table::fifelse(
  test = is_absent > 0, yes = 1L, no = 0L)]
# Flag for DID NOT VOTE
meps_rcv_mandate[, is_novote := fifelse(
  test = is.na(result) & is_absent == 0L, # NO VOTE but PRESENT
  yes = 1L, no = 0L) ]

# sort table
data.table::setkeyv(x = meps_rcv_mandate,
                    cols = c("activity_date", "notation_votingId", "pers_id"))
# sapply(meps_rcv_mandate, function(x) sum(is.na(x)))

# Fill in cols ----------------------------------------------------------------#
cols_tofill <- c("represents", "polgroup_id", "natparty_id")
meps_rcv_mandate <- meps_rcv_mandate |>
  dplyr::group_by(pers_id) |>
  tidyr::fill(tidyselect::any_of(cols_tofill),
              .direction = "down") |>
  dplyr::ungroup() |>
  data.table::as.data.table()


# Fill in cols
# cols_tofill <- c("activity_date", "activity_start_date", "activity_order",
#                  "had_decision_outcome", "decisionAboutId",
#                  "number_of_attendees", "number_of_votes_abstention",
#                  "number_of_votes_against", "number_of_votes_favor",
#                  "activity_label_en", "activity_label_fr", "activity_label_mul",
#                  "comment_en", "comment_fr", "referenceText_en", "referenceText_fr",
#                  "responsible_organization_label_en", "responsible_organization_label_fr",
#                  "headingLabel_en", "headingLabel_fr",
#                  "forms_part_of", "recorded_in_a_realization_of")
# meps_rcv_mandate <- meps_rcv_mandate |>
#   dplyr::group_by(notation_votingId) |>
#   tidyr::fill(tidyselect::any_of(cols_tofill),
#               .direction = "downup") |>
#   dplyr::ungroup() |>
#   data.table::as.data.table()
# sapply(meps_rcv_mandate, function(x) sum(is.na(x))) # check

# Sort data
# data.table::setcolorder(x = meps_rcv_mandate,
#                         neworder = c("activity_date", "activity_start_date", "activity_order",
#                                      "notation_votingId", "mep_name", "result",
#                                      "vote_intention", "is_absent", "is_novote",
#                                      "political_group", "country"))

# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = meps_rcv_mandate,
                   file = here::here("data_out", "meps_rcv_mandate.csv") )

