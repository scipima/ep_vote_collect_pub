###--------------------------------------------------------------------------###
# Process Vote Data ------------------------------------------------------------
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
#' This is a nested data.frame, with several classes of cols.
#' We tackle the flat part first, which gives us the RCV metadata.
#' Then we grab all the dataframe-cols, unnest them, and keep only 3 languages (if available).
#' Finally, we grab the list-cols and unnest them.
#' For this latter class of cols, unnesting them results in a long data.frame.
#' This means that if we merge it back with the metadata, that in turn will result in duplicate rows.
###--------------------------------------------------------------------------###

vote_list_tmp <- vote_list_tmp[!sapply(X = vote_list_tmp, is.null)]

# list_tmp <- NULL
# for (i_data in seq_along( vote_list_tmp) ) {
#     print(i_data)
#     votes_raw <- vote_list_tmp[[i_data]]

process_vote_day <- function(votes_raw = vote_list_tmp[["MTG-PL-2024-02-27"]]) {
    
    #### Flat cols -------------------------------------------------------------
    # sometimes the class of some cols is corrupt - fix it here
    cols_character <- c("id", "activity_date", "activity_id", "activity_start_date",
                        "decision_method", "had_activity_type", "had_decision_outcome",
                        "notation_votingId", "decisionAboutId", "decisionAboutId_XMLLiteral",
                        "decided_on_a_part_of_a_realization_of")
    cols_integer <- c("activity_order", "number_of_attendees", "number_of_votes_abstention",
                      "number_of_votes_against", "number_of_votes_favor")
    # convert cols
    votes_raw <- votes_raw |>
        dplyr::mutate(across(.cols = tidyselect::any_of( cols_character ), as.character),
                      across(.cols = tidyselect::any_of( cols_integer ), as.character),
                      across(.cols = tidyselect::any_of( cols_integer ), as.integer) ) 
    
    # get flat cols
    votes_today <- votes_raw[, names(votes_raw) %in% c(cols_integer, cols_character)] |> 
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
    
    #### Tackle df-cols --------------------------------------------------------
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
    
    # Merge all DF in list ----------------------------------------------------#
    # https://stackoverflow.com/questions/2209258/merge-several-data-frames-into-one-data-frame-with-a-loop
    df_tmp <- Reduce(f = function(x, y) {
        merge(x, y, all = TRUE, by = c("activity_id"))},
        x = list_tmp, accumulate=F)
    # merge back with original flat data
    votes_today <- merge(votes_today, df_tmp, by = c("activity_id"), all = TRUE) |>
        data.table::as.data.table()
    
    #### Tackle list-cols ------------------------------------------------------
    cols_list <- names(votes_raw)[
        sapply(votes_raw, class) %in% c("list")]
    cols_list <- cols_list[
        !cols_list %in% c("had_voter_abstention", "had_voter_against", "had_voter_favor",
                          "had_voter_intended_abstention", "had_voter_intended_against",
                          "had_voter_intended_favor", "type", "decided_on_a_realization_of",
                          "was_motivated_by") ]
    
    if ( length(cols_list > 0)) {
        list_tmp <- lapply(
            X = setNames(object = cols_list, nm = cols_list),
            FUN = function(j_col) {
                votes_raw |>
                    dplyr::select(activity_id, tidyselect::any_of(j_col)) |>
                    dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
                    tidyr::unnest(tidyselect::any_of(j_col), keep_empty = TRUE) } )
        
        # Merge all DF in list ------------------------------------------------#
        # https://stackoverflow.com/questions/2209258/merge-several-data-frames-into-one-data-frame-with-a-loop
        df_tmp <- Reduce(f = function(x, y) {
            merge(x, y, all = TRUE, by = c("activity_id"))},
            x = list_tmp, accumulate=F)
        # merge back with original flat data
        votes_today <- merge(votes_today, df_tmp, by = c("activity_id"), all = TRUE) |>
            data.table::as.data.table() }
    
    # get rid of useless cols
    cols_todelete <- names(votes_today)[
        names(votes_today) %in% c("id", "decisionAboutId_XMLLiteral")]
    votes_today[, c(cols_todelete) := NULL]
    # clean cols
    votes_today[, `:=`(
        activity_date = as.Date(activity_date),
        activity_start_date = lubridate::as_datetime(activity_start_date),
        decision_method = gsub(
            pattern = "http://publications.europa.eu/resource/authority/decision-method/",
            replacement = "", x = decision_method),
        had_activity_type = gsub(
            pattern = "http://publications.europa.eu/resource/authority/event/",
            replacement = "", x = had_activity_type) ) ]
    if ( "had_decision_outcome" %in% names(votes_today) ) {
        votes_today[, had_decision_outcome := gsub(
            pattern = "http://publications.europa.eu/resource/authority/decision-outcome/",
            replacement = "", x = had_decision_outcome) ] }
    
    # return data.frame
    return(votes_today)
}

# test
# p1=process_vote_day()
# p = lapply(X = vote_list_tmp, FUN = function(x) process_vote_day(x))
