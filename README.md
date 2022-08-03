# Benchmarking NHS England Statistics data using Webscraping

## How it works
Each month NHS England publishes data that every NHS Trust (Hospitals and Commuinty Services) submit for the key target indicators for 
Admissions, Emergency Departments and Cancer.

Running this pipeline for the specified financial years will download all relevant data for the metrics above using beautiful soup; by default these will
be saved into the raw_data directory for the project (though production code would likely push directly to a data lake).

Zip files are extracted where appropriate, and the final output of csv and xls* files are saved to the clean_data directory.

## Installing dependencies
```sh
pip3 install -r requirements.txt
```