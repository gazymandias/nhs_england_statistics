import calendar
import httplib2
import bs4
import re
import pandas as pd
import requests
import os
from custom_logging import log
import shutil


def prepare_urls(financial_years_ending: list, rtt: bool, cancer: bool, ae_emergency: bool):
    core_url = "https://www.england.nhs.uk/statistics"
    # exclude month 0 (aka blank)
    # months_list = list(filter(None, list(calendar.month_name)))
    # construct urls and regex matches to find and download files and add as tuples to urls list
    urls = []
    if True in {rtt, cancer, ae_emergency}:
        for year in financial_years_ending:
            financial_year = f"{year - 1}-{abs(year) % 100}"
            if rtt:
                rtt_base_url = f"{core_url}/statistical-work-areas/rtt-waiting-times/rtt-data-" \
                               f"{financial_year} "
                rtt_file_regex = f"{core_url}/wp-content/uploads/sites/.*Full-CSV-data-file.*.zip"
                urls.append(tuple(['rtt', rtt_base_url, rtt_file_regex]))
            if cancer:
                cancer_wt_base_url = f"{core_url}/statistical-work-areas/cancer-waiting-times/"
                cancer_wt_file_regex = f"{core_url}/wp-content/uploads/sites/2/{year}/.*Cancer-Waiting" \
                                       f"-Times.*Data-Extract-Provider.*.xls"
                urls.append(tuple(['cancer', cancer_wt_base_url, cancer_wt_file_regex]))
            if ae_emergency:
                ae_emergency_base_url = f"{core_url}/statistical-work-areas/ae-waiting-times-and" \
                                        f"-activity/ae-attendances-and-emergency-admissions-{financial_year}/ "
                ae_emergency_file_regex = f"{core_url}/wp-content/uploads/sites/.*.csv"
                urls.append(tuple(['ae_emergency', ae_emergency_base_url, ae_emergency_file_regex]))
        log(f"constructed urls for financial years {financial_years_ending}", level="INFO")
        return urls
    else:
        log(f"nothing selected for download!", level="INFO")
        return


def find_download_links(urls: list):
    # Establishes 'the http.' prefix as a web connector.
    http = httplib2.Http()
    # we'll use this list to store the download links we initially parse
    download_list = []
    # TODO change this from httplib2 to requests
    for metric_name, url, regex in urls:
        status, response = http.request(url)
        for link in bs4.BeautifulSoup(response, 'html.parser', parse_only=bs4.SoupStrainer('a', href=True)) \
                .find_all(attrs={'href': re.compile(regex)}):
            download_list.append(tuple([metric_name, link.get('href')]))
    download_set = set(download_list)
    if not download_set:
        log(f"no download links identified!", level="INFO")
        raise
    else:
        return download_set


def download_files(download_set: set):
    # download the raw data (ensuring we aren't downloading the same file more than once)
    for metric_name, url in download_set:
        response = requests.get(url)
        file_name = os.path.basename(url)
        # in production we would most likely be pushing this output to a data lake
        with open(f"raw_data/{metric_name}_{file_name}", mode='wb') as localfile:
            localfile.write(response.content)
        log(f"downloaded {metric_name}: {url}", level="INFO")
    return


def get_files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield os.path.join(path, file)


def unpack_to_clean():
    for raw_file in get_files('raw_data'):
        try:
            shutil.unpack_archive(raw_file, 'clean_data')
        except shutil.ReadError:
            log(f'This file type cannot be extracted further - copying instead: {os.path.basename(raw_file)}',
                level="DEBUG")
            shutil.copy(raw_file, 'clean_data')
            continue
        except Exception as e:
            log(f'Exception: {e}', level="ERROR")
            raise


def main():
    urls = prepare_urls(financial_years_ending=[2020, 2021, 2022], rtt=True, cancer=True, ae_emergency=True)
    download_links = find_download_links(urls)
    download_files(download_links)
    unpack_to_clean()
    log(f'process complete!', level="INFO")


if __name__ == '__main__':
    main()
