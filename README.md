# EP Vote Collection
This repo collects votes in the European Parliament.
It can be used in two different ways.
First, the repo enables the collection of daily votes.
In other words, if the `ep_rcv_today.R` script is run after the voting session ends, it gather all data relative to the daily EP Plenary.
Second, the repo also offers the codes to download and put in tabular format all votes for the current mandate available on the API. 


## EP Daily Votes
To collect and clean the daily votes in the EP, just 2 of the files in the repo are necessary:

* `ep_rcv_today.R` collects all relevant data to today's Plenary Session. 
More precisely, it starts by selecting the last Session from the calendar (in chronological order).
Then grabs and process all the voting data for the current day. 
Finally it merges just the RCV data with current MEPs in the House. 
At the end of this process, 2 files are deposited in the `data_out` folder: `meps_rcv_today.csv` and `votes_today.csv`.
* `aggregate_rcv.R` aggregates the results of the RCVs by EP Political Groups.
The script stores 2 files in the `data_out` folder: `result_bygroup_byrcv.csv`, which provides the tallies of the votes by Group; and `fullresult_bygroup_byrcv.csv`, which augments the data by inclduing not only the votes reported by the EP, but also `absent` MEPs and MEPS who `did not vote` in specific RCVs.


## EP Votes during the 9th mandate
To collect and clean all votes during this mandate, several files must be executed in the following order:

* `ep_rcv_mandate.R` is the master script that executes all other scritpts.
It first gegts the list of all `meetings`.
Then gets all available data from the EP API on these meetings, namely votes.
It calls two functions to clean the data, `process_vote_day.R` and `process_rcv_day.R`, which respectively deal with *votes* and *rcv* (unsurprisingly ...).
Having cleaned the data, the scripts then saves them into 2 files, `votes_dt.csv` (a `wide` files with as many rows as votes on that day), and `rcv_dt.csv` (a very `long` file containing all RCVs). 
* Once `ep_rcv_mandate.R` has collected and cleaned the voting data, it has to combine it with information on the MEPs.
The `meps_api.R` calls the EP API to first download the full list of MEPs during the 9th mandate, and then grab all the supplementary information on each of these MEPs.
In particular, it grabs the `country`, the `national party`, the `political group`, and then the duration of the `mandates`.
It then combines these pieces of information into a single dataframe, `meps_dates_ids.csv`, which lists all the MEPs who have transited through the EP, with each MEP listed for all the dates in which he/she should have been present in the House, as well as his/her `membership`.
* National parties and EP Political Groups feature as integers in the data, so we also have to execute another script - `ep_bodies.R` - to grab the dictionaries for these unique ids.
Bear in mind that the user should always double check these, as mistakes at data entry stage tend to occur, or data are simply missing.
This script spits out 3 tables, `national_parties.csv`, `political_groups.csv`, and `body_id_full.csv`.
* The last code chunks in `ep_rcv_mandate.R` merges several of these different datasets into a single one.
It creates a grid based on the unique combinations of the RCV unique identifiers and the dates, and then merges it with `meps_dates_ids.csv`.
In that way, we create a table where not only EP-registered votes are present, but also the absence and no-vote (i.e. a MEP who is present in the House but decides not to cast a specific vote).
After a bit more cleaning, we save `meps_rcv_mandate.csv` to disk.

As this file accumulates over time and is likely to get large, I decided not to merge it the `votes_dt.csv`, which contains all the metadata.
The user can easily achieve that by left-merging the `meps_rcv_mandate.csv` with `votes_dt.csv` by the shared column, namely `notation_votingId`.


## Data
We extract all data from the [EP Open Data API](https://data.europarl.europa.eu/en/developer-corner/opendata-api).
While the purpose of this repo is just to test the collection of daily data, it can be tweaked to gather more Plenary Sessions, and potentially entire mandates (as long as access is granted through the API). 


## Known issues
The ultimate resource for Votes and RCV should be the [EP finalised minutes](https://www.europarl.europa.eu/RegistreWeb/search/simpleSearchHome.htm?types=PPVD&sortAndOrder=DATE_DOCU_DESC).
Here instead we grab all votes on the same day.
This is prone to *error* and/or *failure*.
So, the user is strongly encouraged to always check the daily data against the official records. 

*Failure*, as the data may not have hit the server yet, and thus our calls go empty.
If that is the case, the code breaks and an error is thrown.

*Error*, as the day may be messy.
For instance, many language translations only accumulates over time.
Usually the only ones readily available are the `mul` (for multilingual, i.e. French), or `.fr` (for French).
Further, there may be duplicate lines. 
MEPs are also given a time frame in which they can report that they pressed the wrong button (this is recorded under `intentions`).
In addition, more columns may be made available over time.

## Execution
The repo hosts a container (`.devcontainer`) which can be deployed through GitHub Codespaces.
For more info, please check [r2u for Codespaces](https://eddelbuettel.github.io/r2u/vignettes/Codespaces/).
In short, a free Codespace account comes with the GitHub registration, within certain constraints (see [here](https://github.com/features/codespaces) for more details).

Remember that some of the datasets are rather long. 
For instance, as of `2024-04-15` the `rcv_dt.csv` is about 12 million rows. 
It is likely that most excel-like software will only load a subset of such data, as it will exceed the limit of rows.
