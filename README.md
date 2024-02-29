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




