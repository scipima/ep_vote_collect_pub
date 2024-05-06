
# get status code from API ----------------------------------------------------#
url_list_tmp <- lapply(
    X = setNames(object = calendar$activity_id,
                 nm = calendar$activity_id),
    FUN = function(i_param) {
        # grid to loop over
        print(i_param) # check
        Sys.sleep(5) # call politely
        api_url <- paste0("https://data.europarl.europa.eu/api/v2/meetings/",
                          i_param,
                          "/decisions?format=application%2Fld%2Bjson&json-layout=framed")
        # Get data from URL
        httr::GET(api_url) } )

# get data from API -----------------------------------------------------------#
get_vote <- function(links = url_list_tmp) {
    future.apply::future_lapply(
        X = links, FUN = function(i_url) {
            print(i_url$url) # check
            if ( httr::status_code(i_url) == 200L ) {
                # Get data from .json
                api_list <- jsonlite::fromJSON(
                    rawToChar(i_url$content) )
                # extract info
                return(api_list$data) } } ) }

# parallelisation -----------------------------------------------------------###
future::plan(strategy = multisession) # Run in parallel on local computer
vote_list_tmp <- get_vote()
future::plan(strategy = sequential) # revert to normal

# remove objects --------------------------------------------------------------#
rm(url_list_tmp)
