###--------------------------------------------------------------------------###
# Daily EP Votes ---------------------------------------------------------------
###--------------------------------------------------------------------------###

rm(list = ls())

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
library(data.table)
library(dplyr)
library(tidyr)
library(tidyselect)
library(future.apply)

# check if dir exists to dump processed files
if ( !dir.exists(here::here("data_out") ) ) {
  dir.create(here::here("data_out") ) }


###--------------------------------------------------------------------------###
## GET Meetings ----------------------------------------------------------------
# EXAMPLE: https://data.europarl.europa.eu/api/v1/plenary-documents?year=2016&format=application%2Fld%2Bjson&offset=0
# create parameters to loop over
api_base <- "https://data.europarl.europa.eu/api/v1"
years <- 2023 : data.table::year(Sys.Date()) # currently, API only has data since 2023
# If more years are needed, change the line above to:
# years <- 2016 : data.table::year(Sys.Date())
# grid to loop over
api_params <- paste0("/meetings?year=", years,
                     "&format=application%2Fld%2Bjson&offset=0")

# Function ------------------------------------------------------------------###
# list_tmp <- vector(mode = "list", length = length(api_params))
get_docs_year <- function(links = api_params) {
  future.apply::future_lapply(
    X = links, FUN = function(param) {
      # for (param in seq_along(api_params)) {  # UNCOMMENT TO TEST LOOP
      # api_url <- paste0(api_base, api_params[param])  # UNCOMMENT TO TEST LOOP
      api_url <- paste0(api_base, param)
      # print(api_url)
      api_raw <- httr::GET(api_url)
      api_list <- jsonlite::fromJSON(
        rawToChar(api_raw$content),
        flatten = TRUE)
      # extract info
      docs_year <- api_list$data
      # list[[param]] <- docs_year # UNCOMMENT TO TEST LOOP
      return(docs_year) } ) }

# parallelisation -----------------------------------------------------------###
future::plan(strategy = multisession) # Run in parallel on local computer
list_tmp <- get_docs_year()
future::plan(strategy = sequential) # revert to normal

# append data ---------------------------------------------------------------###
names(list_tmp) <- years
calendar <- data.table::rbindlist(list_tmp,
                                  use.names=TRUE, fill=TRUE, idcol="year")
# sapply(plenary_documents, function(x) sum(is.na(x)))
# clean data
calendar[, `:=`(year = as.integer(year),
                date = as.Date(gsub(pattern = "eli/dl/event/MTG-PL-",
                                    replacement = "", x = id)))]
# get just the Plenaries that have already taken place, including today
calendar <- calendar[date <= Sys.Date()]
# get identifier for today's Plenary
calendar[, c("type", "id", "had_activity_type") := NULL]

# Remove API objects --------------------------------------------------------###
rm(api_params, list_tmp)


###--------------------------------------------------------------------------###
## GET/meetings/{event-id}/decisions -------------------------------------------
# Returns all decisions in a single EP Meeting --------------------------------#
# EXAMPLE: "https://data.europarl.europa.eu/api/v1/meetings/MTG-PL-2023-07-12/decisions?format=application%2Fld%2Bjson&json-layout=framed"

# get status code from API ----------------------------------------------------#
url_list_tmp <- lapply(
  X = setNames(object = calendar$activity_id, nm = calendar$activity_id),
  FUN = function(i_param) {
    # grid to loop over
    print(i_param) # check
    api_params <- paste0("/meetings/", i_param,
                         "/decisions?format=application%2Fld%2Bjson&json-layout=framed")
    api_url <- paste0(api_base, api_params)
    # Get data from URL
    httr::GET(api_url) } )

# get data from API -----------------------------------------------------------#
vote_list_tmp <- lapply(
  X = url_list_tmp,
  function(i_url) {
    print(i_url$url) # check
    if ( httr::status_code(i_url) != 404 ) {
      # Get data from .json
      api_list <- jsonlite::fromJSON(
        rawToChar(i_url$content))
      # # extract info
      api_list$data } } )





daily_vote_list <-

for (i_data in vote_list_tmp) {



}













































###--------------------------------------------------------------------------###
### Process data ---------------------------------------------------------------

#' This is a nested data.frame, with several classes of cols.
#' We tackle the flat part first, which gives us the RCV metadata.
#' Then we extract and append all votes.
#' Then we grab all the dataframe-cols, unnest them, and keep only 3 languages (if available).
#' Finally, we grab the list-cols and unnest them.
#' For this latter class of cols, unnesting them results in a long data.frame.
#' This means that if we merge it back with the metadata, that in turn will result in duplicate rows.

#### Flat cols -----------------------------------------------------------------
cols_tokeep <- names(votes_raw)[
  sapply(votes_raw, class) %in% c("character", "integer")]
votes_today <- votes_raw[, cols_tokeep] |>
  dplyr::distinct() # DEFENSIVE: there may be duplicate rows
# `type` is the only col we have to unnest wider
votes_today <- votes_raw |>
  dplyr::select(activity_id, type) |>
  dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
  tidyr::unnest_wider(type, names_sep = "_") |>
  dplyr::mutate(type = ifelse(
    test = type_1 %in% "Decision",
    yes = paste(type_1, type_2, sep = "_"),
    no = paste(type_2, type_1, sep = "_") ) ) |>
  dplyr::select(-starts_with("type_")) |>
  dplyr::right_join(
    y = votes_today,
    by = "activity_id")


#### Vote data -----------------------------------------------------------------
# Votes -----------------------------------------------------------------------#
vote_cols <- c("had_voter_abstention", "had_voter_against", "had_voter_favor")
rcv_vote <- lapply(
  X = setNames(object = vote_cols, nm = vote_cols),
  FUN = function(j_col) {
    votes_raw |>
      dplyr::filter( grepl(pattern = "ROLL_CALL_EV", x = decision_method) ) |>
      dplyr::select(activity_id, tidyselect::any_of(j_col)) |>
      dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
      tidyr::unnest( tidyselect::any_of(j_col) ) } ) |>
  data.table::rbindlist(use.names = FALSE, fill = FALSE, idcol = "result") |>
  dplyr::rename(pers_id = had_voter_abstention) |>
  dplyr::distinct() # DEFENSIVE: there may be duplicate rows

# check for duplicates again
df_check <- rcv_vote[, .N, by = list(pers_id, activity_id)]

if (mean(df_check$N) > 1) {
  warning("WATCH OUT: You may have duplicate records")}

# Intentions ------------------------------------------------------------------#
vote_intention_cols <- c("had_voter_intended_abstention", "had_voter_intended_against",
                         "had_voter_intended_favor")
vote_intention_cols <- vote_intention_cols[vote_intention_cols %in% names(votes_raw)]
rcv_vote_intention <- lapply(
  X = setNames(object = vote_intention_cols, nm = vote_intention_cols),
  FUN = function(j_col) {
    votes_raw |>
      dplyr::filter( grepl(pattern = "ROLL_CALL_EV", x = decision_method) ) |>
      dplyr::select(activity_id, tidyselect::any_of(j_col) ) |>
      dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
      tidyr::unnest( tidyselect::any_of(j_col) ) } ) |>
  data.table::rbindlist(use.names = FALSE, fill = FALSE, idcol = "vote_intention") |>
  dplyr::rename(pers_id = 3) |>
  dplyr::distinct() # DEFENSIVE: there may be duplicate rows

# check for duplicates again
df_check <- rcv_vote[, .N, by = list(pers_id, activity_id)]

if (mean(df_check$N) > 1) {
  warning("WATCH OUT: You may have duplicate records")}

# Merge vote with intentions  -------------------------------------------------#
# !! WATCH OUT: This must be a FULL JOIN !!
# This is because sometime MEPs don't have a vote, but do have an intention
rcv_vote <- merge(rcv_vote, rcv_vote_intention,
                  by = c("pers_id", "activity_id"),
                  all = TRUE) |> # !! FULL JOIN HERE - IMPORTANT !!
  dplyr::mutate(pers_id = gsub(pattern = "person/", replacement = "", x = pers_id)) |>
  data.table::as.data.table()


#### Tackle df-cols ------------------------------------------------------------
cols_dataframe <- names(votes_raw)[
  sapply(votes_raw, class) %in% c("data.frame")]
list_tmp <- lapply(
  X = setNames(object = cols_dataframe, nm = cols_dataframe),
  FUN = function(j_col) {
    votes_raw |>
      dplyr::select(activity_id, tidyselect::any_of(j_col)) |>
      dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
      tidyr::unnest(tidyselect::any_of(j_col),
                    keep_empty = TRUE, names_sep = "_") |>
      dplyr::select(
        activity_id,
        contains( c("_en", "_fr", "_mul") ) ) } )

# Merge all DF in list --------------------------------------------------------#
# https://stackoverflow.com/questions/2209258/merge-several-data-frames-into-one-data-frame-with-a-loop
df_tmp <- Reduce(f = function(x, y) {
  merge(x, y, all = TRUE, by = c("activity_id"))},
  x = list_tmp, accumulate=F)
# merge back with original flat data
votes_today <- merge(votes_today, df_tmp, by = c("activity_id"), all = TRUE)


#### Tackle list-cols ----------------------------------------------------------
cols_list <- names(votes_raw)[
  sapply(votes_raw, class) %in% c("list")]
cols_list <- cols_list[
  !cols_list %in% c("had_voter_abstention", "had_voter_against", "had_voter_favor",
                    "had_voter_intended_abstention", "had_voter_intended_against",
                    "had_voter_intended_favor", "type", "decided_on_a_realization_of",
                    "was_motivated_by") ]
list_tmp <- lapply(
  X = setNames(object = cols_list, nm = cols_list),
  FUN = function(j_col) {
    votes_raw |>
      dplyr::select(activity_id, tidyselect::any_of(j_col)) |>
      dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
      tidyr::unnest(tidyselect::any_of(j_col),
                    keep_empty = TRUE) } )

#' The 2 cols `decided_on_a_realization_of` and `was_motivated_by` need to be unnested separately.
#' They provide additional info on amendments, and links between the daily votes.
#' If we include them all in the final voting data we have repeated rows.
#' We treat them together below.

# Merge all DF in list --------------------------------------------------------#
# https://stackoverflow.com/questions/2209258/merge-several-data-frames-into-one-data-frame-with-a-loop
df_tmp <- Reduce(f = function(x, y) {
  merge(x, y, all = TRUE, by = c("activity_id"))},
  x = list_tmp, accumulate=F)
# merge back with original flat data
votes_today <- merge(votes_today, df_tmp, by = c("activity_id"), all = TRUE) |>
  data.table::as.data.table()
votes_today[, c("id", "decisionAboutId_XMLLiteral") := NULL]
# clean cols
votes_today[, `:=`(
  activity_date = as.Date(activity_date),
  activity_start_date = lubridate::as_datetime(activity_start_date),
  decision_method = gsub(
    pattern = "http://publications.europa.eu/resource/authority/decision-method/",
    replacement = "", x = decision_method),
  had_activity_type = gsub(
    pattern = "http://publications.europa.eu/resource/authority/event/",
    replacement = "", x = had_activity_type),
  had_decision_outcome = gsub(
    pattern = "http://publications.europa.eu/resource/authority/decision-outcome/",
    replacement = "", x = had_decision_outcome)
)]

# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = votes_today,
                   file = here::here("data_out", paste0(today, "_votes_today.csv") ) )


#### `decided_on_a_realization_of` and `was_motivated_by` ----------------------
# decided_on_a_realization_of
if("decided_on_a_realization_of" %in% names(votes_raw) ) {
  decided_on_a_realization_of <- votes_raw |>
    dplyr::select(activity_id, decided_on_a_realization_of) |>
    dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
    tidyr::unnest(decided_on_a_realization_of) }

# was_motivated_by
if("was_motivated_by" %in% names(votes_raw) ) {
  was_motivated_by <- votes_raw |>
    dplyr::select(activity_id, was_motivated_by) |>
    dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
    tidyr::unnest(was_motivated_by) }


###--------------------------------------------------------------------------###
# Get final data --------------------------------------------------------------#
# Here it's a left-join because you have to get rid of the EV still in the `votes_today`
rcv_today <- merge(
  x = rcv_vote,
  y = votes_today[grepl(pattern = "ROLL_CALL_EV", x = votes_today$decision_method),],
  by = c("activity_id"), all.x = TRUE, all.y = FALSE) |>
  data.table::as.data.table()
rcv_today[, c("activity_id") := NULL]
rcv_today[result == "had_voter_abstention", result := 0]
rcv_today[result == "had_voter_favor", result := 1]
rcv_today[result == "had_voter_against", result := -1]
rcv_today[, result := as.integer(result)]
rcv_today[vote_intention == "had_voter_intended_abstention", vote_intention := 0]
rcv_today[vote_intention == "had_voter_intended_favor", vote_intention := 1]
rcv_today[vote_intention == "had_voter_intended_against", vote_intention := -1]
rcv_today[, vote_intention := as.integer(vote_intention)]
# checks
# str(rcv_today)
# rcv_today[, .N, by = list(notation_votingId, pers_id)][order(N)] # check, must be always 1
# sapply(rcv_today, function(x) sum(is.na(x)))

# Remove API objects --------------------------------------------------------###
rm(api_raw, api_list, list_tmp, df_check)


###--------------------------------------------------------------------------###
## GET/meps/show-current -------------------------------------------------------
# Returns the list of all active MEPs for today's date
api_raw <- httr::GET(
  url = "https://data.europarl.europa.eu/api/v1/meps/show-current?format=application%2Fld%2Bjson&offset=0")
api_list <- jsonlite::fromJSON(
  rawToChar(api_raw$content),
  flatten = TRUE)
meps_current <- api_list$data |>
  janitor::clean_names() |>
  dplyr::select(
    mep_name = label, pers_id = identifier,
    political_group = api_political_group,
    country = api_country_of_representation)

# Remove API objects --------------------------------------------------------###
rm(api_raw, api_params, api_url, api_list)


###--------------------------------------------------------------------------###
## Merge RCV with MEPs ---------------------------------------------------------
# Create a grid with all MEPs who SHOULD have been present
meps_rcv_grid <- tidyr::expand_grid(
  meps_current,
  notation_votingId = unique(rcv_today$notation_votingId) )

# merge grid with RCV data
meps_rcv_today <- merge(x = meps_rcv_grid,
                        y = rcv_today,
                        by = c("pers_id", "notation_votingId"), all = TRUE) |>
  data.table::as.data.table()

# check
if ( nrow(meps_rcv_today) > nrow(meps_rcv_grid) ) {
  warning("WATCH OUT: You may have duplicate records")}


# Final cleaning --------------------------------------------------------------#
# delete no variance cols
# sapply(meps_rcv_today, function(x) sum(is.na(x)))
# str(meps_rcv_today)
meps_rcv_today[, c("decision_method", "type", "had_activity_type") := NULL]

# Flag for ABSENT
meps_rcv_today[, is_absent := fifelse(
  test = pers_id %in% rcv_today$pers_id, # do we have a vote for MEP?
  yes = 0L, no = 1L) ]
# Flag for DID NOT VOTE
meps_rcv_today[, is_novote := fifelse(
  test = is.na(result) & is_absent == 0L, # NO VOTE but PRESENT
  yes = 1L, no = 0L) ]

# Fill in cols
cols_tofill <- c("activity_date", "activity_start_date", "activity_order",
                 "had_decision_outcome", "decisionAboutId",
                 "number_of_attendees", "number_of_votes_abstention",
                 "number_of_votes_against", "number_of_votes_favor",
                 "activity_label_en", "activity_label_fr", "activity_label_mul",
                 "comment_en", "comment_fr", "referenceText_en", "referenceText_fr",
                 "responsible_organization_label_en", "responsible_organization_label_fr",
                 "headingLabel_en", "headingLabel_fr",
                 "forms_part_of", "recorded_in_a_realization_of")
meps_rcv_today <- meps_rcv_today |>
  dplyr::group_by(notation_votingId) |>
  tidyr::fill(tidyselect::any_of(cols_tofill),
              .direction = "downup") |>
  dplyr::ungroup() |>
  data.table::as.data.table()
# sapply(meps_rcv_today, function(x) sum(is.na(x))) # check

# Sort data
data.table::setkeyv(x = meps_rcv_today, cols = c("activity_start_date", "mep_name"))
data.table::setcolorder(x = meps_rcv_today,
                        neworder = c("activity_date", "activity_start_date", "activity_order",
                                     "notation_votingId", "mep_name", "result",
                                     "vote_intention", "is_absent", "is_novote",
                                     "political_group", "country"))

# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = meps_rcv_today,
                   file = here::here("data_out",
                                     paste0(today, "_meps_rcv_today.csv") ) )

