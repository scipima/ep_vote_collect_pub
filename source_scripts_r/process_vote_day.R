###--------------------------------------------------------------------------###
# Process data ---------------------------------------------------------------
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
#' This is a nested data.frame, with several classes of cols.
#' We tackle the flat part first, which gives us the RCV metadata.
#' Then we extract and append all votes.
#' Then we grab all the dataframe-cols, unnest them, and keep only 3 languages (if available).
#' Finally, we grab the list-cols and unnest them.
#' For this latter class of cols, unnesting them results in a long data.frame.
#' This means that if we merge it back with the metadata, that in turn will result in duplicate rows.
###--------------------------------------------------------------------------###

process_vote_day <- function(votes_raw = vote_list_tmp[["MTG-PL-2023-01-16"]]) {

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
}
