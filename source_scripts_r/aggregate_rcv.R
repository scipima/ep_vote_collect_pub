###--------------------------------------------------------------------------###
# Aggregate EP RCV -------------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' DESCRIPTION.
#' First, we check whether we have a `today` object in the environment.
#' If we do, we can run the daily aggregations.
#' Else, we just run the aggregations for the entire mandate.
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
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

#------------------------------------------------------------------------------#
## Data ------------------------------------------------------------------------
meps_rcv_mandate <- data.table::fread(
  file = here::here("data_out", "meps_rcv_mandate.csv"),
  verbose = TRUE, key = c("rcv_id", "pers_id"))
votes_dt <- data.table::fread(
  file = here::here("data_out", "votes_dt.csv") )
meps_dates_ids <- data.table::fread(
  file = here::here("data_out", "meps_dates_ids.csv") )
country_dict <- data.table::fread(
  file = here::here("data_out", "country_dict.csv") )

# Merge
meps_rcv_mandate <- merge(x = meps_rcv_mandate,
                          y = votes_dt[, list(rcv_id, activity_date)],
                          by = "rcv_id")
meps_rcv_mandate <- merge(x = meps_rcv_mandate,
                          y = meps_dates_ids,
                          by = c("pers_id", "activity_date") )
meps_rcv_mandate <- merge(x = meps_rcv_mandate,
                          y = country_dict,
                          by = c("country_id") )
meps_rcv_mandate[, country_id := NULL]
# sapply(meps_rcv_mandate, function(x) sum(is.na(x) ) )


#------------------------------------------------------------------------------#
## Aggregate RCV results by Political Groups -----------------------------------

# Votes tallies by EP Groups
tally_bygroup_byrcv <- meps_rcv_mandate[
  , list(tally = length(unique(pers_id))),
  keyby = list(rcv_id, polgroup_id, result) ]
# Votes tallies by National Parties
tally_bygroup_byparty_byrcv <- meps_rcv_mandate[
  , list(tally = length(unique(pers_id))),
  keyby = list(rcv_id, polgroup_id, natparty_id, result) ]

# write data to disk --------------------------------------------------------#
data.table::fwrite(x = tally_bygroup_byrcv,
                   file = here::here("data_out", "tally_bygroup_byrcv.csv"))
data.table::fwrite(x = tally_bygroup_byparty_byrcv,
                   file = here::here("data_out", "tally_bygroup_byparty_byrcv.csv"))


#------------------------------------------------------------------------------#

#' Uncomment the lines below if you also want to extract tables for the Groups' majorities

# calculate majority
majority_bygroup_byrcv <- tally_bygroup_byrcv[
  result >= -1,
  list(vote_max = max(tally, na.rm = TRUE),
       votes_sum = sum(tally, na.rm = TRUE)),
  keyby = list(rcv_id, polgroup_id)]

# check for ties
majority_bygroup_byrcv[, .N, by = list(rcv_id, polgroup_id)][order(N)]


# write data to disk --------------------------------------------------------#
data.table::fwrite(x = majority_bygroup_byrcv,
                   file = here::here("data_out", "majority_bygroup_byrcv.csv"))
