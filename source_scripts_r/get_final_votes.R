#------------------------------------------------------------------------------#
# Extract Final Votes ----------------------------------------------------------
#------------------------------------------------------------------------------#

#' While the EP Rules of Procedure mention *final votes*, it is much harder to measures them as there is no explicit flag in the data for those cases
#' We revert then to string matching in the vote description to try to catch as many occurrences as we can.
#' However, this solution is suboptimal, as our indicator is subject to typos during data entry, or incompleteness of the strings we use to capture final votes. 

#------------------------------------------------------------------------------#
## Read data in ----------------------------------------------------------------
# read in file if not present already
if ( !exists("votes_dt") ) {
  votes_dt <- data.table::fread(file = here::here(
    "data_out", "votes_dt.csv"),
  na.strings = c(NA_character_, "") ) }
# sapply(X = votes_dt, function(x) sum(is.na(x))) # check NAs


#------------------------------------------------------------------------------#
## Final votes ----------------------------------------------------------------
# identify whether vote is final based on guess of what VoteWatch did -------###
# c("single vote", "vote:.*", "as a whole", "Procedural vote", "Election", "joint text")
votes_dt[
  grepl(pattern = "vote unique|vote:.*|ensembl*e du texte|Election|Proposition de la Commission|Procédure d'approbation|Approbation|Demande de vote",
        x = activity_label_fr, ignore.case = T, perl = T)
  | grepl(pattern = "Single vote|vote:.*|text as a whole|Election|Commission Proposal|Consent procedure|Request to vote",
          x = activity_label_en, ignore.case = T, perl = T),
  is_final := 1L]
votes_dt[is.na(is_final), is_final := 0L]
