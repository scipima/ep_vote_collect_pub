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
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c("data.table", "dplyr", "tidyr", "tidyselect", "future.apply",
                        "httr", "here", "lubridate", "janitor", "jsonlite") )

# repo set-up -----------------------------------------------------------------#
# check if dir exists to dump processed files
if ( !dir.exists( here::here("data_out") ) ) {
  dir.create( here::here("data_out") ) }
if ( !dir.exists( here::here("data_in") ) ) {
  dir.create( here::here("data_in") ) }
if ( !dir.exists( here::here("data_in", "meeting_decision_json") ) ) {
  dir.create( here::here("data_in", "meeting_decision_json") ) }


###--------------------------------------------------------------------------###
## GET Meetings ----------------------------------------------------------------

#' Here we're getting a list of all the Plenaries during the mandate.

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

### Download all these files if very long, make sure we do it rarely ----------#
# check whether data already exists
if ( file.exists( here::here("data_out", "votes_dt.csv") ) ) {
  # get date of last version
  mtime <- as.Date(file.info(
    here::here("data_out", "votes_dt.csv"))[["mtime"]])
  if ( (Sys.Date() - mtime) == 0L ) {
    # read data ---------------------------------------------------------------#
    votes_dt <- data.table::fread( here::here("data_out", "votes_dt.csv") )
    rcv_dt <- data.table::fread( here::here("data_out", "rcv_dt.csv") )
  } else {
    # If the data is older than today, recreate data from source --------------#
    source(file = here::here("source_scripts_r", "get_meetings_decisions.R") )
  }
} else {
  # If the data is not there, create data from source -------------------------#
  source(file = here::here("source_scripts_r", "get_meetings_decisions.R") )
}
rm(mtime)


###--------------------------------------------------------------------------###
## GET/meps/{mep-id} -----------------------------------------------------------
# Returns a single MEP for the specified mep ID -------------------------------#

#' Get clean data on MEPs' membership and mandate duration.

### Download all these files if very long, make sure we do it rarely ----------#
# check whether data already exists
if ( file.exists( here::here("data_out", "meps_dates_ids.csv") ) ) {
  # get date of last version
  mtime <- as.Date(file.info(
    here::here("data_out", "meps_dates_ids.csv"))[["mtime"]])
  if ( (Sys.Date() - mtime) == 0L ) {
    # read data ---------------------------------------------------------------#
    meps_dates_ids <- data.table::fread( here::here("data_out", "meps_dates_ids.csv") )
  } else {
    # If the data is older than today, recreate data from source --------------#
    source(file = here::here("source_scripts_r", "api_meps.R") )
  }
} else {
  # If the data is not there, create data from source -------------------------#
  source(file = here::here("source_scripts_r", "api_meps.R") )
}
rm(mtime)
# sapply(meps_dates_ids, function(x) sum(is.na(x))) # check


###--------------------------------------------------------------------------###
## GET/corporate-bodies/{body-id} ----------------------------------------------
# Returns a single EP Corporate Body for the specified body ID ----------------#

#' Get look-up tables for MEPs' memberships

### Download all these files if very long, make sure we do it rarely ----------#
# check whether data already exists
if ( file.exists( here::here("data_out", "national_parties.csv") ) ) {
  # get date of last version
  mtime <- as.Date(file.info(
    here::here("data_out", "national_parties.csv"))[["mtime"]])
  if ( (Sys.Date() - mtime) == 0L ) {
    # read data ---------------------------------------------------------------#
    national_parties <- data.table::fread( here::here("data_out", "national_parties.csv") )
    political_groups <- data.table::fread( here::here("data_out", "political_groups.csv") )
  } else {
    # If the data is older than today, recreate data from source --------------#
    source(file = here::here("source_scripts_r", "api_bodies.R") )
  }
} else {
  # If the data is not there, create data from source -------------------------#
  source(file = here::here("source_scripts_r", "api_bodies.R") )
}
rm(mtime)


###--------------------------------------------------------------------------###

#' Load join functions

source(file = here::here("source_scripts_r", "join_functions.R"))
###--------------------------------------------------------------------------###


###--------------------------------------------------------------------------###
## Merge RCV with MEPs ---------------------------------------------------------
# read in file if not present already
if ( !exists("meps_dates_ids") ) {
  meps_dates_ids <- data.table::fread(file = here::here(
    "data_out", "meps_dates_ids.csv") ) }

# Create a grid with all MEPs who SHOULD have been present, by date and rcv_id
meps_rcv_grid <- merge(
  x = meps_dates_ids[, list(activity_date, pers_id)],
  y = unique(rcv_dt[,  list(activity_date, notation_votingId) ] ),
  by = "activity_date", all = TRUE, allow.cartesian = TRUE)
# dim(meps_rcv_grid)
# sapply(meps_rcv_grid, function(x) sum(is.na(x)))


# merge grid with RCV data
meps_rcv_mandate <- merge(
  x = meps_rcv_grid[, list(activity_date, notation_votingId, pers_id)],
  y = rcv_dt,
  by = c("activity_date", "notation_votingId", "pers_id"),
  all = TRUE) |>
  data.table::as.data.table()
# sapply(meps_rcv_mandate, function(x) sum(is.na(x)))


# check
if ( nrow(meps_rcv_mandate) > nrow(meps_rcv_grid) ) {
  warning("WATCH OUT: You may have duplicate records") }


###--------------------------------------------------------------------------###
# Final cleaning --------------------------------------------------------------#

#' ABSENT: if both `result` and `vote_intention` are NA, then `is_absent` = 1
meps_rcv_mandate[, is_absent := data.table::fifelse(
  test = is.na(vote_intention) & is.na(result), yes = 1L, no = 0L)]
# table(meps_rcv_mandate$is_absent)

#' True absent are absent the entire day, so take the average of `is_absent`
meps_rcv_mandate[, is_absent_avg := mean(is_absent, na.rm = TRUE),
                 by = list(activity_date, pers_id) ]
# head( sort( unique(meps_rcv_mandate$is_absent_avg) ) )

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
# table(meps_rcv_mandate$vote_intention, meps_rcv_mandate$result, exclude = NULL) # check

# Recode
meps_rcv_mandate[is_absent == 1L, result := -3L]
meps_rcv_mandate[is_novote == 1L, result := -2L]

# Vote data dictionary --------------------------------------------------------#
data.frame(
  result = -3 : 1,
  result_fct = c("absent", "no_vote", "against", "abstain", "for") ) |>
  data.table::fwrite(file = here::here("data_out", "vote_dict.csv"))


#------------------------------------------------------------------------------#
# Get the last configuration of the EP
data.table::fwrite(
  x = unique(meps_rcv_mandate[
    activity_date == max(activity_date, na.rm = TRUE),
    list(pers_id, natparty_id, polgroup_id, country)]),
  file = here::here("data_out", "meps_lastplenary.csv"), verbose = TRUE)


# Drop cols --------------------------------------------------------------------#
# We've kept the date col so far because we need it to create several vars above
meps_rcv_mandate[, c("activity_date", "is_novote", "is_absent") := NULL]


# sort table ------------------------------------------------------------------#
data.table::setkeyv(x = meps_rcv_mandate,
                    cols = c("notation_votingId", "pers_id"))
# sapply(meps_rcv_mandate, function(x) sum(is.na(x)))

# Rename cols -----------------------------------------------------------------#
data.table::setnames(x = meps_rcv_mandate,
                     old = c("notation_votingId"), new = c("rcv_id"),
                     skip_absent = TRUE)

# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = meps_rcv_mandate,
                   file = here::here("data_out", "meps_rcv_mandate.csv"),
                   verbose = TRUE)


# remove objects --------------------------------------------------------------#
rm( meps_dates_ids, meps_mandate, meps_rcv_grid )
