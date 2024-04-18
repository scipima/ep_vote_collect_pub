###--------------------------------------------------------------------------###
# Aggregate EP RCV -------------------------------------------------------
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
#' DESCRIPTION.
#' First, we check whether we have a `today` object in the environment.
#' If we do, we can run the daily aggregations. 
#' Else, we just run the aggregations for the entire mandate. 
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
library(data.table)


if (exists("today")) {
  ###------------------------------------------------------------------------###
  ## Data ----------------------------------------------------------------------
  meps_rcv_today <- data.table::fread(file = here::here(
    "data_out", paste0(today, "_meps_rcv_today.csv")))
  
  ###------------------------------------------------------------------------###
  ## Aggregate RCV results by Political Groups ---------------------------------
  result_bygroup_byrcv <- meps_rcv_today[!is.na(result),
                                         list(Sum = length(unique(pers_id))), 
                                         keyby = list(rcv_id, political_group, result) ]
  meps_rcv_today[is_absent == 1L, result_full := "absent"]
  meps_rcv_today[is_novote == 1L, result_full := "no vote"]
  meps_rcv_today[result == 1L, result_full := "for"]
  meps_rcv_today[result == 0L, result_full := "abstain"]
  meps_rcv_today[result == -1L, result_full := "against"]
  fullresult_bygroup_byrcv <- meps_rcv_today[, list(Sum = length(unique(pers_id))), 
                                             keyby = list(rcv_id, political_group, result_full) ]
  
  # write data to disk --------------------------------------------------------#
  data.table::fwrite(x = meps_rcv_today,
                     file = here::here("data_out", paste0(today, "_meps_rcv_today.csv")))
  data.table::fwrite(x = result_bygroup_byrcv,
                     file = here::here("data_out", paste0(today, "_result_bygroup_byrcv.csv")))
  data.table::fwrite(x = fullresult_bygroup_byrcv,
                     file = here::here("data_out", paste0(today, "_fullresult_bygroup_byrcv.csv")))
}

###------------------------------------------------------------------------###
## Data ----------------------------------------------------------------------
meps_rcv_mandate <- data.table::fread(
  file = here::here("data_out", "meps_rcv_mandate.csv"),
  verbose = TRUE, key = c("activity_date", "rcv_id", "pers_id"))

###------------------------------------------------------------------------###
## Aggregate RCV results by Political Groups ---------------------------------
meps_rcv_mandate[is_absent == 1L, result_full := "absent"]
meps_rcv_mandate[is_novote == 1L, result_full := "no vote"]
meps_rcv_mandate[result == 1L, result_full := "for"]
meps_rcv_mandate[result == 0L, result_full := "abstain"]
meps_rcv_mandate[result == -1L, result_full := "against"]
table(meps_rcv_mandate$result_full, exclude = NULL)
tally_bygroup_byrcv <- meps_rcv_mandate[
  , list(tally = length(unique(pers_id))), 
  keyby = list(rcv_id, polgroup_id, result_full) ]
tally_bygroup_byparty_byrcv <- meps_rcv_mandate[
  , list(tally = length(unique(pers_id))), 
  keyby = list(rcv_id, country, polgroup_id, natparty_id, result_full) ]

# write data to disk --------------------------------------------------------#
data.table::fwrite(x = tally_bygroup_byrcv,
                   file = here::here("data_out", "tally_bygroup_byrcv.csv"))
data.table::fwrite(x = tally_bygroup_byparty_byrcv,
                   file = here::here("data_out", "tally_bygroup_byparty_byrcv.csv"))


###------------------------------------------------------------------------###
#' Uncomment the lines below if you also want to extract tables for the Groups' majorities

# # calculate majority
majorityfull_bygroup_byrcv <- tally_bygroup_byrcv[
  , list(vote_max = max(tally, na.rm = TRUE),
         votes_sum = sum(tally, na.rm = TRUE)),
  keyby = list(rcv_id, polgroup_id)]
majority_bygroup_byrcv <- tally_bygroup_byrcv[
  result_full %in% c("for", "against", "abstain"),
  list(vote_max = max(tally, na.rm = TRUE),
       votes_sum = sum(tally, na.rm = TRUE)),
  keyby = list(rcv_id, polgroup_id)]
# 
# # check for ties
majorityfull_bygroup_byrcv[, .N, by = list(rcv_id, polgroup_id)][order(N)]
majority_bygroup_byrcv[, .N, by = list(rcv_id, polgroup_id)][order(N)]
# 
# # write data to disk --------------------------------------------------------#
data.table::fwrite(x = majorityfull_bygroup_byrcv,
                   file = here::here("data_out", "majorityfull_bygroup_byrcv.csv"))
data.table::fwrite(x = majority_bygroup_byrcv,
                   file = here::here("data_out", "majority_bygroup_byrcv.csv"))

###------------------------------------------------------------------------###
# Get the last configuration of the EP
data.table::fwrite(
  x = unique(meps_rcv_mandate[
    activity_date == max(activity_date, na.rm = TRUE),
    list(pers_id, natparty_id, polgroup_id, country)]),
  file = here::here("data_out", "meps_lastplenary.csv"), verbose = TRUE)

