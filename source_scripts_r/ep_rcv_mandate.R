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

# repo set-up -----------------------------------------------------------------#
# check if dir exists to dump processed files
if ( !dir.exists(here::here("data_out") ) ) {
  dir.create(here::here("data_out") ) }


###--------------------------------------------------------------------------###
## GET Meetings ----------------------------------------------------------------
# EXAMPLE: # https://data.europarl.europa.eu/api/v2/meetings?year=2022&format=application%2Fld%2Bjson&offset=0&limit=50
# create parameters to loop over
years <- 2019 : data.table::year(Sys.Date())
# grid to loop over
api_params <- paste0("https://data.europarl.europa.eu/api/v2/meetings?year=",
                     years,
                     "&format=application%2Fld%2Bjson&offset=0")

# get data from API -----------------------------------------------------------#
get_meetings_json <- lapply(
  X = api_params,
  FUN = function(api_url) {
    print(api_url)
    api_raw <- httr::GET(api_url)
    return(api_raw) } )
names(get_meetings_json) <- years

# get data from JSON ----------------------------------------------------------#
json_list <- lapply(
  X = get_meetings_json,
  function(i_json) {
    print(i_json$url) # check
    if ( httr::status_code(i_json) == 200L ) {
      # Get data from .json
      api_list <- jsonlite::fromJSON(
        rawToChar(i_json$content))
      # # extract info
      return(api_list$data) } } )


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
rm(api_params, get_meetings_json, json_list)


###--------------------------------------------------------------------------###
## GET/meetings/{event-id}/decisions -------------------------------------------
# Returns all decisions in a single EP Meeting --------------------------------#
# EXAMPLE: "https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-2023-07-12/decisions?format=application%2Fld%2Bjson&json-layout=framed"

# loop to get all decisions
resp_list <- lapply(
  X = setNames(object = calendar$activity_id,
               nm = calendar$activity_id),
  FUN = function(i_param) {
    print(i_param)
    # Creae an API request
    req <- httr2::request("https://data.europarl.europa.eu/api/v2") |>
      httr2::req_url_path_append("meetings") |>
      httr2::req_url_path_append(i_param) |>
      httr2::req_url_path_append("/decisions?format=application%2Fld%2Bjson&json-layout=framed")
    # Add time-out and ignore error
    resp <- req |>
      httr2::req_error(is_error = ~FALSE) |>
      httr2::req_throttle(10 / 60) |>
      httr2::req_perform()
    # If not an error, download and make available in ENV
    if ( httr2::resp_status(resp) == 200L) {
      resp_body <- resp |>
        httr2::resp_body_json()
      # store this on disk to manually check if needs be
      jsonlite::write_json(x = resp_body$data,
                           path = here::here("data_in", "meeting_decision_json",
                                             paste0(i_param, ".json") ) )
      return(resp_body$data)
    } } )

# get rid of NULL items in list
resp_list <- resp_list[!sapply(X = resp_list, is.null)]

###--------------------------------------------------------------------------###
# Source functions ------------------------------------------------------------#

#' These 2 functions clean, respectively, the vote data and the RCV data.

source(file = here::here("source_scripts_r", "process_vote_day.R"))
source(file = here::here("source_scripts_r", "process_list_longdf.R"))
source(file = here::here("source_scripts_r", "process_rcv_day.R"))
###--------------------------------------------------------------------------###

### Get Votes and RCV DFs ------------------------------------------------------
# append votes ----------------------------------------------------------------#
votes_dt <- lapply(X = resp_list, FUN = function(x) process_vote_day(x) ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE, idcol = "plenary_id")
# sapply(votes_dt, function(x) sum(is.na(x))) # check
# quick and dirty way to get doc_id out of labels
votes_dt[, doc_id := data.table::fifelse(
  test = is.na(activity_label_en),
  yes = stringr::str_extract(
    string = activity_label_fr,
    pattern = "[A-Z][8-9]-\\d{4}/\\d{4}|[A-Z]{2}-[A-Z]9-\\d{4}/\\d{4}"),
  no = stringr::str_extract(
    string = activity_label_en,
    pattern = "[A-Z][8-9]-\\d{4}/\\d{4}|[A-Z]{2}-[A-Z]9-\\d{4}/\\d{4}") ) ]
votes_dt[, notation_votingId := as.integer(notation_votingId)]
data.table::setnames(x = votes_dt, old = c("notation_votingId"), new = c("rcv_id"))

# append rcv ------------------------------------------------------------------#
rcv_dt <- lapply(X = vote_list_tmp, FUN = function(x) process_rcv_day(x) ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE, idcol = "plenary_id")
rcv_dt[, activity_date := as.Date(gsub(pattern = "MTG-PL-",
                                       replacement = "", x = plenary_id))]
rcv_dt[, plenary_id := NULL]
# sapply(rcv_dt, function(x) sum(is.na(x))) # check

# write data to disk ----------------------------------------------------------#
# votes
data.table::fwrite(x = votes_dt,
                   file = here::here("data_out", "votes_dt.csv") )
# just doc_id and rcv_id
data.table::fwrite(x = unique(votes_dt[, list(rcv_id, doc_id)]),
                   file = here::here("data_out", "rcvid_docid.csv") )
# rcv
data.table::fwrite(x = rcv_dt,
                   file = here::here("data_out", "rcv_dt.csv") )


###--------------------------------------------------------------------------###
# Get DF in list-cols ---------------------------------------------------------#
# process_decided_on_a_realization_of
decided_on_a_realization_of <- lapply(
  X = vote_list_tmp,
  FUN = function(x) process_decided_on_a_realization_of(x) ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE, idcol = "plenary_id")
data.table::fwrite(x = decided_on_a_realization_of,
                   file = here::here("data_out", "decided_on_a_realization_of.csv") )

# process_was_motivated_by
motivated_by <- lapply(
  X = vote_list_tmp,
  FUN = function(x) process_was_motivated_by(x) ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE, idcol = "plenary_id")
data.table::fwrite(x = motivated_by,
                   file = here::here("data_out", "motivated_by.csv") )


# remove objects --------------------------------------------------------------#
rm(vote_list_tmp)

###--------------------------------------------------------------------------###
# Source script ---------------------------------------------------------------#

#' Get clean data on MEPs' membership and mandate duration.

source(file = here::here("source_scripts_r", "api_meps.R"))
# sapply(meps_dates_ids, function(x) sum(is.na(x))) # check

#' Get look-up tables for MEPs' memberships, and fetch `join_functions.R`

source(file = here::here("source_scripts_r", "api_bodies.R"))
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


# data entry mistake
rcv_dt[pers_id == 6840L, pers_id := 197443L]

# merge grid with RCV data
meps_rcv_mandate <- merge(x = meps_rcv_grid, y = rcv_dt,
                          by = c("activity_date", "notation_votingId", "pers_id"),
                          all = TRUE) |>
  data.table::as.data.table()
# sapply(meps_rcv_mandate, function(x) sum(is.na(x)))


# check
if ( nrow(meps_rcv_mandate) > nrow(meps_rcv_grid) ) {
  warning("WATCH OUT: You may have duplicate records") }


###--------------------------------------------------------------------------###
# Final cleaning --------------------------------------------------------------#
#'ABSENT: if both `result` and `vote_intention` are NA, then `is_absent` = 1
meps_rcv_mandate[, is_absent := data.table::fifelse(
  test = is.na(vote_intention) & is.na(result), yes = 1L, no = 0L)]
# unique(meps_rcv_mandate$is_absent)
#' True absent are absent the entire day, so take the average of `is_absent`
meps_rcv_mandate[, is_absent_avg := mean(is_absent, na.rm = TRUE),
                 by = list(activity_date, pers_id)]
# head(sort(unique(meps_rcv_mandate$is_absent_avg)))
#' Convert `is_absent` to binary
meps_rcv_mandate[, is_absent := data.table::fifelse(
  test = is_absent_avg == 1, yes = 1L, no = 0L) ]
meps_rcv_mandate[, c("is_absent_avg") := NULL]
# Flag for DID NOT VOTE
meps_rcv_mandate[, is_novote := data.table::fifelse(
  test = is.na(result) & is.na(vote_intention) & is_absent == 0L, # NO VOTE but PRESENT
  yes = 1L, no = 0L) ]
# table(meps_rcv_mandate$is_absent, meps_rcv_mandate$is_novote, exclude = NULL) # check
# table(meps_rcv_mandate$result, meps_rcv_mandate$is_absent, exclude = NULL) # check
# table(meps_rcv_mandate$result, meps_rcv_mandate$is_novote, exclude = NULL) # check
# table(meps_rcv_mandate$vote_intention, meps_rcv_mandate$is_absent, exclude = NULL) # check
# table(meps_rcv_mandate$vote_intention, meps_rcv_mandate$is_novote, exclude = NULL) # check

# sort table
data.table::setkeyv(x = meps_rcv_mandate,
                    cols = c("activity_date", "notation_votingId", "pers_id"))
# sapply(meps_rcv_mandate, function(x) sum(is.na(x)))

# Fill in cols ----------------------------------------------------------------#
#' There are 2 MEPs who have a mistmatch between national party and political group membership.
#' They are: https://www.europarl.europa.eu/meps/en/228604/KAROLIN_BRAUNSBERGER-REINHOLD/history/9#detailedcardmep; https://www.europarl.europa.eu/meps/en/226260/KAROLIN_BRAUNSBERGER-REINHOLD/history/9#detailedcardmep
#' We fix the data gaps here.

cols_tofill <- c("country", "polgroup_id", "natparty_id")
meps_rcv_mandate <- meps_rcv_mandate |>
  dplyr::group_by(pers_id) |>
  tidyr::fill(tidyselect::any_of(cols_tofill),
              .direction = "downup") |>
  dplyr::ungroup() |>
  data.table::as.data.table()
data.table::setnames(meps_rcv_mandate, old = c("notation_votingId"), new = c("rcv_id"))

# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = meps_rcv_mandate,
                   file = here::here("data_out", "meps_rcv_mandate.csv"),
                   verbose = TRUE)
