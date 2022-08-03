import calendar
import httplib2
import bs4
import re
import pandas as pd
import requests
import os
from custom_logging import log
import shutil

# exclude month 0 (aka blank)
months_list = list(filter(None, list(calendar.month_name)))
extraction_period_fy_ending = [2020, 2021, 2022]

# construct the urls and the regex matches we will use to find and download the published statistics files and add as tuples to urls list
urls = []
for year in extraction_period_fy_ending:
    financial_year = f"{year - 1}-{abs(year) % 100}"

    rtt_base_url = f"https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-{financial_year}"
    rtt_file_regex = "https://www.england.nhs.uk/statistics/wp-content/uploads/sites/.*Full-CSV-data-file.*.zip"
    
    urls.append(tuple(['rtt', rtt_base_url, rtt_file_regex]))

    cancer_wt_base_url = "https://www.england.nhs.uk/statistics/statistical-work-areas/cancer-waiting-times/"
    cancer_wt_file_regex = f"https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/{year}/.*Cancer-Waiting-Times.*Data-Extract-Provider.*.xls"
    urls.append(tuple(['cancer', cancer_wt_base_url, cancer_wt_file_regex]))

    ae_emergency_base_url = f"https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/ae" \
                   f"-attendances-and-emergency-admissions-{financial_year}/"
    ae_emergency_file_regex = "https://www.england.nhs.uk/statistics/wp-content/uploads/sites/.*.csv"
    urls.append(tuple(['ae_emergency', ae_emergency_base_url, ae_emergency_file_regex]))

# Establishes 'the http.' prefix as a web connector.
http = httplib2.Http()

# we'll use this list to store the download links we initially parse
download_list = []

# TODO change this from httplib2 to requests
for metric_name, url, regex in urls:
    # print(url, regex)
    status, response = http.request(url)
    for link in bs4.BeautifulSoup(response, 'html.parser', parse_only=bs4.SoupStrainer('a', href=True)) \
            .find_all(attrs={'href': re.compile(regex)}):
        download_list.append(tuple([metric_name, link.get('href')]))

download_set = set(download_list)
log(f"urls to download{download_set}", level="DEBUG")

# download the raw data (ensuring we aren't downloading the same file more than once)
for metric_name, url in download_set:
    response = requests.get(url)
    file_name = os.path.basename(url)
    # in production we would most likely be pushing this output to a data lake
    with open(f"raw_data/{metric_name}_{file_name}", mode='wb') as localfile:     
        localfile.write(response.content)


def get_files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield os.path.join(path, file)

for file in get_files('raw_data'):
    try:
        shutil.unpack_archive(file, 'clean_data')
    except shutil.ReadError:
        log(f'This file type cannot be extracted further - copying instead: {os.path.basename(file)}', level="DEBUG")
        shutil.copy(file, 'clean_data')
    except Exception as e:
        log(f'Exception: {e}', level="ERROR")
