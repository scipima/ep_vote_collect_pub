###--------------------------------------------------------------------------###
## BODIES --------------------------------------------------------------

###--------------------------------------------------------------------------###
#' Here we grab all the bodies appearing in the MEPs' history.
#' For the EP administration, `bodies` are various things, from `Committees`, to `Political Groups`, to `national parties`.
#' For the current purposes, we're just interested in the last 2 bodies.
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
library(data.table)
library(httr)
library(jsonlite)
library(future.apply)


###--------------------------------------------------------------------------###
## Data -------------------------------------------------------------------
meps_dates_ids <- data.table::fread(here::here("data_out", "meps_dates_ids.csv"))

# Grab vector of bodies in the current data
bodies_id <- na.omit( c( unique(meps_dates_ids$polgroup_id), 
                         unique( meps_dates_ids$natparty_id) ) )
# remove object
rm(meps_dates_ids)

# grid to loop over
api_params <- paste0("https://data.europarl.europa.eu/api/v1/corporate-bodies/", 
                     bodies_id,
                     "?format=application%2Fld%2Bjson&json-layout=framed")

# Function ------------------------------------------------------------------###
get_body_id <- function(links = api_params) {
  future.apply::future_lapply(
    X = links, FUN = function(api_url) {
      print(api_url)
      api_raw <- httr::GET(api_url)
      api_list <- jsonlite::fromJSON(
        rawToChar(api_raw$content),
        flatten = TRUE)
      # extract info
      return(api_list[["data"]]) } ) }

# perform parallelised check
future::plan(strategy = multisession) # Run in parallel on local computer
list_tmp <- get_body_id()
future::plan(strategy = sequential) # revert to normal

# append list and transform into DF
body_id_full <- data.table::rbindlist(l = list_tmp,
                                      use.names = TRUE, fill = TRUE) |>
  dplyr::select(identifier, label, classification,
                ends_with(".en"), ends_with(".fr")) |>
  dplyr::mutate(classification = gsub(
    pattern = "http://publications.europa.eu/resource/authority/corporate-body-classification/",
    replacement = "", x = classification) ) %>% 
  janitor::clean_names()


# write to disk ---------------------------------------------------------------#
data.table::fwrite(x = body_id_full,
                   here::here("data_out", "body_id_full.csv"))
# write NATIONAL PARTIES
data.table::fwrite(x = body_id_full[grepl(pattern = "NP", x = classification),
                                    -c("classification")],
                   here::here("data_out", "national_parties.csv"))
# write POLITICAL GROUPS
data.table::fwrite(x = body_id_full[grepl(pattern = "EP_GROUP", x = classification),
                                    -c("classification")],
                   here::here("data_out", "political_groups.csv"))

# remove objects
rm(list_tmp, api_params, bodies_id)
