###--------------------------------------------------------------------------###
# Similarity and Affinity Functions --------------------------------------------
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
## House' majority -------------------------------------------------------------

#' Get the House majorities
get_house_majority <- function(data_in = meps_rcv_mandate) {
    # check if data_in is DT
    if (!data.table::is.data.table(data_in)) {
        data_in <- data.table::as.data.table(data_in) }
    
        who_won <- data_in[,
            list(who_won = .N),
            by = list(rcv_id, result) ]
    
    # ensure that data is sorted so that subsequent filtering is correct ----###
    data.table::setorder(x = who_won,
                         rcv_id, -who_won)
    
    # Check if max vote is tied ---------------------------------------------###
    # Is duplicate equal to max?
    who_won[, `:=`(is_duplicated = duplicated(who_won),
                   is_max = who_won == max(who_won)),
            by = list(rcv_id)]
    # If yes, then tied
    who_won[, is_tied := data.table::fifelse(
        test = (is_duplicated + is_max == 2L), 
        yes = 1L, no = 0L)]
    # Apply tied to all party rows
    who_won[, to_drop := mean(is_tied, na.rm = TRUE),
            by = list(rcv_id)]
    # drop rows
    who_won <- who_won[to_drop == 0]
    # drop cols
    who_won[, c("is_duplicated", "is_tied", "is_max", "to_drop") := NULL]
    
    # filter ----------------------------------------------------------------###
    who_won <- who_won[,
                       head(.SD, 1L),
                       keyby = list(rcv_id)
    ]
}

###--------------------------------------------------------------------------###
## Groups' majority ------------------------------------------------------------

#' Get the Groups' majorities

get_polgroup_majority <- function(data_in = meps_rcv_mandate) {
    # check if data_in is DT
    if (!data.table::is.data.table(data_in)) {
        data_in <- data.table::as.data.table(data_in) }

        who_won <- data_in[,
            list(who_won = .N),
            by = list(rcv_id, polgroup_id, result) ]
    
    # ensure that data is sorted so that subsequent filtering is correct ----###
    data.table::setorder(x = who_won,
                         rcv_id, polgroup_id, -who_won)
    
    # Check if max vote is tied ---------------------------------------------###
    # Is duplicate equal to max?
    who_won[, `:=`(is_duplicated = duplicated(who_won),
                   is_max = who_won == max(who_won)),
            by = list(rcv_id, polgroup_id)]
    # If yes, then tied
    who_won[, is_tied := data.table::fifelse(
        test = (is_duplicated + is_max == 2L), 
        yes = 1L, no = 0L)]
    # Apply tied to all party rows
    who_won[, to_drop := mean(is_tied, na.rm = TRUE),
            by = list(rcv_id, polgroup_id)]
    # drop rows
    who_won <- who_won[to_drop == 0]
    # drop cols
    who_won[, c("is_duplicated", "is_tied", "is_max", "to_drop") := NULL]
    
    # filter ----------------------------------------------------------------###
    who_won <- who_won[,
                       head(.SD, 1L),
                       keyby = list(rcv_id, polgroup_id)
    ]
}
