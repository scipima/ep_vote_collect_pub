###--------------------------------------------------------------------------###
# Join Functions ---------------------------------------------------------------
###--------------------------------------------------------------------------###

# rm(list = ls())

###--------------------------------------------------------------------------###
#' DESCRIPTION.
#' These are a set of convenience functions to left-join the unique ids in the EP databases with the data dictionaries. 
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
library(data.table)
library(dplyr)
library(tidyr)


###--------------------------------------------------------------------------###
### National Parties -----------------------------------------------------------
national_parties <- data.table::fread(
  here::here("data_out", "national_parties.csv"))
national_parties[, c("alt_label_en", "alt_label_fr", "pref_label_fr") 
                 := NULL]
# Get all parties ID for Independents
natparties_independent_ids <- national_parties$identifier[
  grepl(
    pattern = "^-$|Indépendant|Indépendent|Independent|Independente|Independiente", 
    x = national_parties$pref_label_en)]


###--------------------------------------------------------------------------###
### Political Groups -----------------------------------------------------------
political_groups <- data.table::fread(
  here::here("data_out", "political_groups.csv"))
political_groups[, c("alt_label_en", "alt_label_fr", "pref_label_fr") 
                 := NULL]
# Fix data entry mistakes
political_groups[identifier == 5151L,
                 `:=`(label = "The Left",
                      pref_label_en = "The Left group in the European Parliament - GUE/NGL")]
# Harmonise labels
political_groups[identifier == 5153L, `:=`(label = "EPP")]
political_groups[identifier == 5155L, `:=`(label = "Greens/EFA")]


###--------------------------------------------------------------------------###
### MEPs mandate ---------------------------------------------------------------
meps_mandate <- data.table::fread(
  here::here("data_out", "meps_mandate.csv") )


###--------------------------------------------------------------------------###
### Function to merge LHS data with political labels ---------------------------
join_polit_labs <- function(data_in) {
  if ( "natparty_id" %in% names(data_in) ) {
    # get national party labels
    data_in <- merge(x = data_in, 
                     y = national_parties[, list(identifier, national_party = label)],
                     by.x = "natparty_id", by.y = "identifier", 
                     all.x = TRUE, all.y = FALSE) }
  
  if ( "polgroup_id" %in% names(data_in) ) {
    # get political groups labels
    data_in <- merge(x = data_in, 
                     y = political_groups[, list(identifier, political_group = label)],
                     by.x = "polgroup_id", by.y = "identifier", 
                     all.x = TRUE, all.y = FALSE) }
  return(data_in) }

### Function to merge LHS data with MEPs' names --------------------------------
join_meps_names <- function(data_in) {
  # get MEPs' names
  data_in <- merge(x = data_in, 
                   y = meps_mandate[, list(identifier, mep_name = label)],
                   by.x = "pers_id", by.y = "identifier", 
                   all.x = TRUE, all.y = FALSE) 
  return(data_in) }
