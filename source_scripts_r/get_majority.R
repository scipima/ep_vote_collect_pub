###--------------------------------------------------------------------------###
# Similarity and Affinity Functions --------------------------------------------
###--------------------------------------------------------------------------###


###--------------------------------------------------------------------------###
## Data -------------------------------------------------------------------
# rcv_attendance <- data.table::fread(
#     file = here::here("data_out", "rcv_attendance.csv"),
#     na.strings = c(NA_character_, "") )
# delete NAs and other values
# dim(rcv_attendance)
# get rid of attendance grid
# rcv_attendance <- rcv_attendance[!grepl(pattern = "NoVote_", x = result), ]
# there cannot be NAs in the RCV identifier
# rcv_attendance <- rcv_attendance[!is.na(rcv_id), ]
# rcv_attendance <- rcv_attendance[rcv_id != "", ]
# dim(rcv_attendance)


###--------------------------------------------------------------------------###
## House' majority -------------------------------------------------------------

#' Get the House majorities, with 2 possible filtering conditions:
#' either based on RCVS, or on dossiers ids

get_house_majority <- function(data_in = rcv_attendance,
                               rcv_ids = NULL, 
                               dossier_ids = NULL) {
    # check if data_in is DT
    if (!data.table::is.data.table(data_in)) {
        data_in <- data.table::as.data.table(data_in) }
    
    # Get the Groups' majority by rcv_id ------------------------------------###
    ## NO rcv_id & NO DOSSIER_IDS filter? -------------------------------------#
    if (missing(rcv_ids) & missing(dossier_ids)) {
        who_won <- data_in[,
                           list(who_won = .N),
                           by = list(rcv_id, doc_id, result) ] }
    
    ## YES rcv_id but NO DOSSIER_IDS filter? ----------------------------------#
    if (!missing(rcv_ids) & missing(dossier_ids)) {
        who_won <- data_in[rcv_id %in% rcv_ids,
                           list(who_won = .N),
                           by = list(rcv_id, doc_id, result) ] }
    
    ## NO rcv_id but YES DOSSIER_IDS filter? ----------------------------------#
    if (missing(rcv_ids) & !missing(dossier_ids)) {
        who_won <- data_in[doc_id %in% dossier_ids,
                           list(who_won = .N),
                           by = list(rcv_id, doc_id, result) ] }
    
    ## YES rcv_id & YES DOSSIER_IDS filter? -----------------------------------#
    if (!missing(rcv_ids) & !missing(dossier_ids)) {
        who_won <- data_in[
            rcv_id %in% rcv_ids &
                doc_id %in% dossier_ids,
            list(who_won = .N),
            by = list(rcv_id, doc_id, result) ] }
    
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
                       keyby = list(rcv_id, doc_id)
    ]
}

###--------------------------------------------------------------------------###
### Test ----------------------------------------------------------------------#
# p <- get_house_majority()
# p1 <- get_house_majority(
#     rcv_ids = c(142082, 148340),
#     dossier_ids = c(
#         "A9-0088/2022", "A9-0089/2022", "A9-0092/2022",
#         "A9-0071/2022", "A9-0082/2022", "A9-0128/2022") )
###--------------------------------------------------------------------------###


###--------------------------------------------------------------------------###
## Groups' majority ------------------------------------------------------------

#' Get the Groups' majorities, with 2 possible filtering conditions:
#' either based on RCVS, or on dossiers ids

get_polgroup_majority <- function(data_in = rcv_attendance,
                                  rcv_ids = NULL, 
                                  dossier_ids = NULL) {
    # check if data_in is DT
    if (!data.table::is.data.table(data_in)) {
        data_in <- data.table::as.data.table(data_in) }
    
    # Get the Groups' majority by rcv_id ------------------------------------###
    ## NO rcv_id & NO DOSSIER_IDS filter? -------------------------------------#
    if (missing(rcv_ids) & missing(dossier_ids)) {
        who_won <- data_in[,
                           list(who_won = .N),
                           by = list(rcv_id, doc_id, polgroup_id, result) ] }
    
    ## YES rcv_id but NO DOSSIER_IDS filter? ----------------------------------#
    if (!missing(rcv_ids) & missing(dossier_ids)) {
        who_won <- data_in[rcv_id %in% rcv_ids,
                           list(who_won = .N),
                           by = list(rcv_id, doc_id, polgroup_id, result) ] }
    
    ## NO rcv_id but YES DOSSIER_IDS filter? ----------------------------------#
    if (missing(rcv_ids) & !missing(dossier_ids)) {
        who_won <- data_in[doc_id %in% dossier_ids,
                           list(who_won = .N),
                           by = list(rcv_id, doc_id, polgroup_id, result) ] }
    
    ## YES rcv_id & YES DOSSIER_IDS filter? -----------------------------------#
    if (!missing(rcv_ids) & !missing(dossier_ids)) {
        who_won <- data_in[
            rcv_id %in% rcv_ids &
                doc_id %in% dossier_ids,
            list(who_won = .N),
            by = list(rcv_id, doc_id, polgroup_id, result) ] }
    
    # ensure that data is sorted so that subsequent filtering is correct ----###
    data.table::setorder(x = who_won,
                         rcv_id, doc_id, polgroup_id, -who_won)
    
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
                       keyby = list(rcv_id, doc_id, polgroup_id)
    ]
}

###--------------------------------------------------------------------------###
### Test ----------------------------------------------------------------------#
# p <- get_polgroup_majority()
# p1 <- get_polgroup_majority(
#     rcv_ids = c(142082, 148340),
#     dossier_ids = c(
#         "A9-0088/2022", "A9-0089/2022", "A9-0092/2022",
#         "A9-0071/2022", "A9-0082/2022", "A9-0128/2022") )


###--------------------------------------------------------------------------###
###--------------------------------------------------------------------------###
## Parties' majority -----------------------------------------------------------

#' Get the Groups' majorities, with 2 possible filtering conditions:
#' either based on RCVS, or on dossiers ids

get_natparty_majority <- function(data_in = rcv_attendance,
                                  rcv_ids = NULL, 
                                  dossier_ids = NULL) {
    # check if data_in is DT
    if (!data.table::is.data.table(data_in)) {
        data_in <- data.table::as.data.table(data_in) }
    
    # Get the Parties' majority by rcv_id -----------------------------------###
    ## NO rcv_id & NO DOSSIER_IDS filter? -------------------------------------#
    if (missing(rcv_ids) & missing(dossier_ids)) {
        who_won <- data_in[,
                           list(who_won = .N),
                           by = list(rcv_id, doc_id, country, polgroup_id,
                                     natparty_id, result) ] }
    
    ## YES rcv_id but NO DOSSIER_IDS filter? ----------------------------------#
    if (!missing(rcv_ids) & missing(dossier_ids)) {
        who_won <- data_in[
            rcv_id %in% rcv_ids,
            list(who_won = .N),
            by = list(rcv_id, doc_id, country, polgroup_id, natparty_id, result) ] }
    
    ## NO rcv_id but YES DOSSIER_IDS filter? ----------------------------------#
    if (missing(rcv_ids) & !missing(dossier_ids)) {
        who_won <- data_in[
            doc_id %in% dossier_ids,
            list(who_won = .N),
            by = list(rcv_id, doc_id, country, polgroup_id, natparty_id, result) ] }
    ## YES rcv_id & YES DOSSIER_IDS filter? -----------------------------------#
    if (!missing(rcv_ids) & !missing(dossier_ids)) {
        who_won <- data_in[
            rcv_id %in% rcv_ids &
                doc_id %in% dossier_ids,
            list(who_won = .N),
            by = list(rcv_id, doc_id, country, polgroup_id, natparty_id, result) ]
    }
    
    # ensure that data is sorted so that subsequent filtering is correct ----###
    data.table::setorder(x = who_won,
                         rcv_id, doc_id, country, polgroup_id, natparty_id, -who_won)
    
    #  check if max vote is tied --------------------------------------------###
    # check if duplicate is equal to max
    who_won[, `:=`(is_duplicated = duplicated(who_won),
                   is_max = who_won == max(who_won)),
            by = list(rcv_id, country, polgroup_id, natparty_id)]
    # if yes, then tied
    who_won[, is_tied := data.table::fifelse(
        test = (is_duplicated + is_max == 2L), 
        yes = 1L, no = 0L)]
    # apply tied to all party rows
    who_won[, to_drop := mean(is_tied, na.rm = TRUE),
            by = list(rcv_id, country, polgroup_id, natparty_id)]
    # drop rows
    who_won <- who_won[to_drop == 0]
    # drop cols
    who_won[, c("is_duplicated", "is_tied", "is_max", "to_drop") := NULL]
    
    
    # filter ----------------------------------------------------------------###
    who_won <- who_won[,
                       head(.SD, 1L),
                       keyby = list(rcv_id, doc_id, country, polgroup_id, natparty_id) ]
}


###--------------------------------------------------------------------------###
### Test ----------------------------------------------------------------------#
# p <- get_natparty_majority()
# p1 <- get_natparty_majority(
#     rcv_ids = c(142082, 148340),
#     dossier_ids = c(
#         "A9-0088/2022", "A9-0089/2022", "A9-0092/2022",
#         "A9-0071/2022", "A9-0082/2022", "A9-0128/2022") )
