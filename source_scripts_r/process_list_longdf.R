###--------------------------------------------------------------------------###
# Process `decided_on_a_realization_of` and `was_motivated_by` -----------------
###--------------------------------------------------------------------------###

# decided_on_a_realization_of
process_decided_on_a_realization_of <- function(votes_raw = vote_list_tmp[["MTG-PL-2024-02-27"]]) {
  if("decided_on_a_realization_of" %in% names(votes_raw) ) {
    decided_on_a_realization_of <- votes_raw |>
      dplyr::select(activity_id, decided_on_a_realization_of) |>
      dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
      tidyr::unnest(decided_on_a_realization_of) 
    # return data.frame
    return(decided_on_a_realization_of) } }


# decided_on_a_realization_of
recorded_in_a_realization_of <- function(votes_raw = vote_list_tmp[["MTG-PL-2024-02-27"]]) {
  if("recorded_in_a_realization_of" %in% names(votes_raw) ) {
    recorded_in_a_realization_of <- votes_raw |>
      dplyr::select(activity_id, recorded_in_a_realization_of) |>
      dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
      tidyr::unnest(recorded_in_a_realization_of) 
    # return data.frame
    return(recorded_in_a_realization_of) } }
# recorded_in_a_realization_of()

# was_motivated_by
process_was_motivated_by <- function(votes_raw = vote_list_tmp[["MTG-PL-2024-02-27"]]) {
  if("was_motivated_by" %in% names(votes_raw) ) {
    was_motivated_by <- votes_raw |>
      dplyr::select(activity_id, was_motivated_by) |>
      dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
      tidyr::unnest(was_motivated_by)
    # return data.frame
    return(was_motivated_by) } }
