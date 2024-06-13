###--------------------------------------------------------------------------###
# Get Vote Files & Process -----------------------------------------------------
###--------------------------------------------------------------------------###

#' This script collects all the vote files.
#' Having collected them, the script proceeds to process them and store the data on disk.


###--------------------------------------------------------------------------###
## GET/meetings/{event-id}/decisions -------------------------------------------
# Returns all decisions in a single EP Meeting --------------------------------#
# EXAMPLE: "https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-2023-07-12/decisions?format=application%2Fld%2Bjson&json-layout=framed"

### Loop to get all decisions --------------------------------------------------
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
        # Add time-out and ignore error before performing request
        resp <- req |>
            httr2::req_error(is_error = ~FALSE) |> # ignore error, we deal with it below
            httr2::req_throttle(30 / 60) |> # call politely
            httr2::req_perform()
        # If not an error, download and make available in ENV
        if ( httr2::resp_status(resp) == 200L) {
            resp_body <- resp |>
                # https://cran.r-project.org/web/packages/jsonlite/vignettes/json-aaquickstart.html
                httr2::resp_body_json( simplifyDataFrame = TRUE )
            # store this on disk to manually check if needs be
            jsonlite::write_json(x = resp_body$data,
                                 path = here::here("data_in", "meeting_decision_json",
                                                   paste0(i_param, ".json") ) )
            return(resp_body$data)
        } } )

# get rid of NULL items in list
resp_list <- resp_list[!sapply(X = resp_list, is.null)]

###--------------------------------------------------------------------------###
#### Check for breaking changes in API -----------------------------------------
source(file = here::here("source_scripts_r", "check_vote_content.R"))

###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
## Process all files -----------------------------------------------------------

#' These 2 functions clean, respectively, the vote data and the RCV data.

source(file = here::here("source_scripts_r", "process_vote_day.R"))
source(file = here::here("source_scripts_r", "process_list_longdf.R"))
source(file = here::here("source_scripts_r", "process_rcv_day.R"))
###--------------------------------------------------------------------------###


###--------------------------------------------------------------------------###
### Append & Clean Votes and RCV DFs -------------------------------------------
# append & clean votes --------------------------------------------------------#
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
# rename col
data.table::setnames(x = votes_dt, old = c("notation_votingId"), new = c("rcv_id"))


# append & clean rcv ----------------------------------------------------------#
rcv_dt <- lapply(X = resp_list, FUN = function(x) process_rcv_day(x) ) |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE, idcol = "plenary_id")
rcv_dt[, activity_date := as.Date(gsub(pattern = "MTG-PL-",
                                       replacement = "", x = plenary_id))]
rcv_dt[, plenary_id := NULL]
# sapply(rcv_dt, function(x) sum(is.na(x))) # check


###--------------------------------------------------------------------------###
## Write data to disk ----------------------------------------------------------
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
    X = resp_list,
    FUN = function(x) process_decided_on_a_realization_of(x) ) |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE, idcol = "plenary_id")
data.table::fwrite(x = decided_on_a_realization_of,
                   file = here::here("data_out", "decided_on_a_realization_of.csv") )

# process_was_motivated_by
motivated_by <- lapply(
    X = resp_list,
    FUN = function(x) process_was_motivated_by(x) ) |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE, idcol = "plenary_id")
data.table::fwrite(x = motivated_by,
                   file = here::here("data_out", "motivated_by.csv") )


#------------------------------------------------------------------------------#
# Get list of final votes
source(file = here::here("source_scripts_r", "get_final_votes.R"))


# remove objects --------------------------------------------------------------#
rm(resp_list)
