###--------------------------------------------------------------------------###
# Process `decided_on_a_realization_of` and `was_motivated_by` -----------------
###--------------------------------------------------------------------------###

# decided_on_a_realization_of
process_decided_on_a_realization_of <- function(votes_raw = resp_list[["MTG-PL-2024-02-27"]]) {
  if("decided_on_a_realization_of" %in% names(votes_raw) ) {
    decided_on_a_realization_of <- votes_raw |>
      dplyr::select(activity_id, decided_on_a_realization_of) |>
      dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
      tidyr::unnest(decided_on_a_realization_of)
    # return data.frame
    return(decided_on_a_realization_of) } }


# was_motivated_by
process_was_motivated_by <- function(votes_raw = resp_list[["MTG-PL-2024-02-27"]]) {
  if("was_motivated_by" %in% names(votes_raw) ) {
    was_motivated_by <- votes_raw |>
      dplyr::select(activity_id, was_motivated_by) |>
      dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
      tidyr::unnest(was_motivated_by, names_sep = "_")
    # return data.frame
    return(was_motivated_by) } }
