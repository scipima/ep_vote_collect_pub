###--------------------------------------------------------------------------###
# Aggregate Daily EP RCV -------------------------------------------------------
###--------------------------------------------------------------------------###

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

###--------------------------------------------------------------------------###
## Data ------------------------------------------------------------------------
meps_rcv_today <- data.table::fread(file = here::here(
  "data_out", paste0(today, "_meps_rcv_today.csv")))

###--------------------------------------------------------------------------###
## Aggregate RCV results by Political Groups -----------------------------------
result_bygroup_byrcv <- meps_rcv_today[!is.na(result),
                                       list(Sum = length(unique(pers_id))), 
                                       keyby = list(notation_votingId, political_group, result) ]
meps_rcv_today[is_absent == 1L, full_result := "absent"]
meps_rcv_today[is_novote == 1L, full_result := "no vote"]
meps_rcv_today[result == 1L, full_result := "for"]
meps_rcv_today[result == 0L, full_result := "abstain"]
meps_rcv_today[result == -1L, full_result := "against"]
fullresult_bygroup_byrcv <- meps_rcv_today[, list(Sum = length(unique(pers_id))), 
                                           keyby = list(notation_votingId, political_group, full_result) ]

# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = meps_rcv_today,
                   file = here::here("data_out", paste0(today, "_meps_rcv_today.csv")))
data.table::fwrite(x = result_bygroup_byrcv,
                   file = here::here("data_out", paste0(today, "_result_bygroup_byrcv.csv")))
data.table::fwrite(x = fullresult_bygroup_byrcv,
                   file = here::here("data_out", paste0(today, "_fullresult_bygroup_byrcv.csv")))