###--------------------------------------------------------------------------###
# Check List of Vote Files -----------------------------------------------------
###--------------------------------------------------------------------------###

#' This script checks the list of all column names in the vote files.
#' The colnames are hard coded, and then we check if new cols are included.

# This is the list of all the columns we can retrieve from all the votes as of 2024-05-06
all_colnames <- c(
    "activity_date", "activity_id", "activity_label", "activity_order", "activity_start_date",
    "comment", "decided_on_a_part_of_a_realization_of", "decided_on_a_realization_of",
    "decision_method", "decisionAboutId", "decisionAboutId_XMLLiteral", "had_activity_type",
    "had_decision_outcome", "had_voter_abstention", "had_voter_against", "had_voter_favor",
    "had_voter_intended_abstention", "had_voter_intended_against", "had_voter_intended_favor",
    "headingLabel", "id", "inverse_consists_of", "notation_votingId", "note",
    "number_of_attendees", "number_of_votes_abstention", "number_of_votes_against",
    "number_of_votes_favor", "recorded_in_a_realization_of", "referenceText",
    "responsible_organization_label", "type", "was_motivated_by")

if ( exists("resp_list") ) {
    colnames_rawlist <- sapply(X = resp_list, FUN = names, simplify = TRUE) |>
        unlist() |>
        unique()
    if ( all( colnames_rawlist %in% all_colnames ) ) {
        message("No new columns. You're good to go.")
    } else {
        message("You have new columns. Check again for changes in the API. Also check for any downstream effects this may have on the processing functions")
    }
} else {
    list_json_files <- list.files(path = here::here("data_in", "meeting_decision_json"),
                                  pattern = ".json")
    list_json_raw <- lapply(list_json_files, function(x)
        jsonlite::read_json(path = here::here("data_in", "meeting_decision_json", x),
                            simplifyDataFrame = TRUE ) )
    colnames_rawlist <- sapply(X = list_json_raw, FUN = names, simplify = TRUE) |>
        unlist() |>
        unique()
    if ( all( colnames_rawlist %in% all_colnames ) ) {
        message("No new columns. You're good to go.")
    } else {
        message("You have new columns. Check again for changes in the API. Also check for any downstream effects this may have on the processing functions")
    }
}

