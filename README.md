# ep_vote_day_collect
This repo tests the collection of daily votes in the European Parliament.
Currently, there are just 2 files in the repo:

* `ep_rcv_today.R` collects all relevant data to today's Plenary Session. 
More precisely, it starts by selecting the last Session from the calendar.
Then grabs and process all the voting data. 
Finally it merges just the RCV data with current MEPs in the House. 
At the end of this process, 2 files are deposited in the `data_out` folder: `meps_rcv_today.csv` and `votes_today.csv`.
* `aggregate_rcv.R` aggregates the results of the RCVs by EP Political Groups.
The script stores 2 files in the `data_out` folder: `result_bygroup_byrcv.csv`, which provides the tallies fo the votes by Group; and `fullresult_bygroup_byrcv.csv`, which augment the data by inclduing not only the votes reported by the EP, but also `absent` MEPs and MEPS who `did not vote` in specific RCVs.


## Data
We extract all data from the [EP Open Data API](https://data.europarl.europa.eu/en/developer-corner/opendata-api).
While the purpose of this repo is just to test for daily data, it can be easily tweaked to gather more Plenary Sessions, and potentially entire mandates (as long as access is granted through the API). 


## Known issues
The ultimate resource for Votes and RCV should be the EP finalised minutes.
Here instead we grab all votes on the same day.
This is prone to *error* and/or *failure*.
So, the user is strongly encouraged to always check the daily data against the official records. 

Failure, as the data may not have hit the server yet, and thus our calls go empty.
If that is the case, the code breaks and an error is thrown.

Error, as the day may be messy.
For instance, many language translations only accumulates over time.
Usually the only ones readily available are the `mul` (for multilingual, i.e. French), or `.fr` (for French).
Further, there may be duplicate lines. 
MEPs are also given a time frame in which they can report that they pressed the wrong button (this is recorded under `intentions`).
In addition, more columns may be made available over time.


