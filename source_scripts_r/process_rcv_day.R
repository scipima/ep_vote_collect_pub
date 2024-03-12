###--------------------------------------------------------------------------###
# Process RCV Data ------------------------------------------------------------
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
#' RCV are list-cols, which in the original records are appended one below the other.
#' We replicate the same structure here.
#' Intentions are instead provided as separate cols.
#' Please pay attention to the fact that intentions are not always expressed.
#' However, when they are, the data has to be merged in FULL JOIN.
#' This is because some MEPs may not have a vote for a given RCV, but only an intention.
###--------------------------------------------------------------------------###

process_rcv_day <- function(votes_raw = vote_list_tmp[["MTG-PL-2022-05-19"]]) {

  # Votes ---------------------------------------------------------------------#
  vote_cols <- c("had_voter_abstention", "had_voter_against", "had_voter_favor")
  vote_cols <- vote_cols[vote_cols %in% names(votes_raw)]

  rcv_vote <- lapply(
    X = setNames(object = vote_cols, nm = vote_cols),
    FUN = function(j_col) {
      votes_raw |>
        dplyr::filter( grepl(pattern = "ROLL_CALL_EV", x = decision_method) ) |>
        dplyr::select(notation_votingId, tidyselect::any_of(j_col)) |>
        dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
        tidyr::unnest( tidyselect::any_of(j_col) ) } ) |>
    data.table::rbindlist(use.names = FALSE, fill = FALSE, idcol = "result") |>
    dplyr::rename(pers_id = had_voter_abstention) |>
    dplyr::distinct() # DEFENSIVE: there may be duplicate rows

  # check for duplicates
  df_check <- rcv_vote[, .N, by = list(pers_id, notation_votingId)]
  if (mean(df_check$N) > 1) {
    warning("WATCH OUT: You may have duplicate records")}

  # Intentions ----------------------------------------------------------------#
  vote_intention_cols <- c("had_voter_intended_abstention", "had_voter_intended_against",
                           "had_voter_intended_favor")
  vote_intention_cols <- vote_intention_cols[vote_intention_cols %in% names(votes_raw)]
  if ( length(vote_intention_cols) > 0 ) {
    rcv_vote_intention <- lapply(
      X = setNames(object = vote_intention_cols, nm = vote_intention_cols),
      FUN = function(j_col) {
        votes_raw |>
          dplyr::filter( grepl(pattern = "ROLL_CALL_EV", x = decision_method) ) |>
          dplyr::select(notation_votingId, tidyselect::any_of(j_col) ) |>
          dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
          tidyr::unnest( tidyselect::any_of(j_col) ) } ) |>
      data.table::rbindlist(use.names = FALSE, fill = FALSE, idcol = "vote_intention") |>
      dplyr::rename(pers_id = 3) |>
      dplyr::distinct() # DEFENSIVE: there may be duplicate rows

    # check for duplicates
    df_check <- rcv_vote_intention[, .N, by = list(pers_id, notation_votingId)]
    if (mean(df_check$N) > 1) {
      warning("WATCH OUT: You may have duplicate records") }

    # Merge vote with intentions  ---------------------------------------------#
    # !! WATCH OUT: This must be a FULL JOIN !!
    # This is because sometime MEPs don't have a vote, but do have an intention
    rcv_vote <- merge(rcv_vote, rcv_vote_intention,
                      by = c("pers_id", "notation_votingId"),
                      all = TRUE) # !! FULL JOIN HERE - IMPORTANT !!
    # check for duplicates again
    df_check <- rcv_vote[, .N, by = list(pers_id, notation_votingId)]
    if (mean(df_check$N) > 1) {
      warning("WATCH OUT: You may have duplicate records")}
  }

  # recode & clean   -------------------------------------------------#
  rcv_vote[, pers_id := as.integer(
    gsub(pattern = "person/", replacement = "", x = pers_id))]
  rcv_vote[result == "had_voter_abstention", result := 0]
  rcv_vote[result == "had_voter_favor", result := 1]
  rcv_vote[result == "had_voter_against", result := -1]
  rcv_vote[, `:=`(result = as.integer(result),
                  notation_votingId = as.integer(notation_votingId) ) ]

  if ( length(vote_intention_cols) > 0 ) {
    rcv_vote[vote_intention == "had_voter_intended_abstention", vote_intention := 0]
    rcv_vote[vote_intention == "had_voter_intended_favor", vote_intention := 1]
    rcv_vote[vote_intention == "had_voter_intended_against", vote_intention := -1]
    rcv_vote[, vote_intention := as.integer(vote_intention)] }

  # return data.frame
  return(rcv_vote)
}

# test
# p1=process_rcv_day()
# p = lapply(X = vote_list_tmp, FUN = function(x) process_rcv_day(x))

