# pickle_exiobase
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)
[![Contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg)](resources/docs/CONTRIBUTING.md)


# Code for parsing and aggregating Exiobase V3.3 CSV .txt files into pickle
Contains the following

* parse_mrSUTs
  	- Parse all SUTs from EXIOBASE and outputs them as pickles to facilitate operations.
  	- It also adds regional label EU or ROW
  	- Restructures labels

* agg_MrSUTs: aggregates and separates them by EU and ROW.


## Notes on this code
While this code gets the job done, its memory footprint is not optimized, it is overcoded and the user needs to modify link references manually. 
Future work should include memory footprint optimization and packaging for deployment.
For the moment anybody can use it but needs to be patient when running "parse_mrSUTs"

Please report any issues you may encounter or improve 
