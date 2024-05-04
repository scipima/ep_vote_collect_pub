###--------------------------------------------------------------------------###
# EP Procedures ----------------------------------------------------------------
###--------------------------------------------------------------------------###

# rm(list = ls())

#' The script grabs all procedural identifiers from the EP API.
#' REF: https://data.europarl.europa.eu/en/home; https://data.europarl.europa.eu/en/developer-corner/opendata-api

###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
library(tidyr)
library(dplyr)
library(lubridate)
library(data.table)
library(httr)
library(httr2)
library(jsonlite)
library(future.apply)


###--------------------------------------------------------------------------###
## GET/procedures --------------------------------------------------------------
# Returns the list of all EP Procedures
# EXAMPLE: https://data.europarl.europa.eu/api/v2/procedures?format=application%2Fld%2Bjson&offset=0&limit=50

req <- httr2::request("https://data.europarl.europa.eu/api/v2") |>
    httr2::req_url_path_append("procedures?format=application%2Fld%2Bjson&offset=0") |>
    httr2::req_perform() |>
    httr2::resp_body_json()
head(req$data)

# check)
procedures_df <- req$data |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
    dplyr::mutate(process_type = gsub(pattern = "def/ep-procedure-types/",
                                    replacement = "", x = process_type) ) |> 
  dplyr::select(-type)
# check
# sapply(procedures_df, function(x) sum(is.na(x)))

# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = procedures_df,
                   file = here::here("data_out", "procedures.csv") )

# Remove API objects --------------------------------------------------------###
rm(req)


###--------------------------------------------------------------------------###
## GET/procedures/{proc-id} ----------------------------------------------------
# Returns a single EP Procedure for the specified proc-ID
# EXAMPLE: https://data.europarl.europa.eu/api/v2/procedures/2018-0218?format=application%2Fld%2Bjson

# get procedures IDs
process_ids <- sort(unique(procedures_df$process_id))

# loop to get all decisions
resp_list <- lapply(
  X = setNames(object = process_ids,
               nm = process_ids),
  FUN = function(i_param) {
    print(i_param)
    # Creae an API request
    req <- httr2::request("https://data.europarl.europa.eu/api/v2") |>
      httr2::req_url_path_append("procedures") |>
      httr2::req_url_path_append(i_param) |>
      httr2::req_url_path_append("?format=application%2Fld%2Bjson")
    # Add time-out and ignore error
    resp <- req |>
      httr2::req_error(is_error = ~FALSE) |>
      httr2::req_throttle(20 / 60) |>
      httr2::req_perform()
    # If not an error, download and make available in ENV
    if ( httr2::resp_status(resp) == 200L) {
      resp_body <- resp |>
        httr2::resp_body_json()
      return(resp_body$data)
    } } )

# append list
process_ids <- data.table::rbindlist(l = resp_list, use.names = TRUE, fill = TRUE)


