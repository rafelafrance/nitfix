"""Crawl Tropicos search portal for data from Notes from Nature expeditions."""

import pandas as pd
from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import lib.db as db


def crawl_tropicos_website():
    """Find data on the Tropicos web site."""
    cxn = db.connect()

    searches = get_search_terms(cxn)
    get_tropicos_data(searches[:2])


def get_search_terms(cxn):
    """Query Notes from Nature data for search terms."""
    sql = """
        SELECT sample_id, last_name, collection_no, collected_by
          FROM nfn_data
         WHERE COALESCE(last_name, '') <> ''
           AND COALESCE(collection_no, '') <> ''
      ORDER BY last_name, collection_no
    """
    return pd.read_sql(sql, cxn).set_index('sample_id')


def get_tropicos_data(searches):
    driver = webdriver.Chrome()

    for index, search in searches.iterrows():
        print(index)
        print(search.last_name)
        _load_page(driver)
        _enter_search(driver, search.last_name, search.collection_no)


def _enter_search(driver, last_name, collection_no):
    COLLECTOR = 'ctl00$MainContentPlaceHolder$seniorCollectorTextBox'
    NUMBER = 'ctl00$MainContentPlaceHolder$collectionNumberTextBox'
    SEARCH = 'ctl00$MainContentPlaceHolder$searchButton'

    collector = driver.find_element_by_name(COLLECTOR)
    collector.clear()
    collector.send_keys(last_name)

    number = driver.find_element_by_name(NUMBER)
    number.clear()
    number.send_keys(collection_no)

    driver.find_element_by_name(SEARCH).click()


def _load_page(driver):
    driver.get("http://www.tropicos.org/SpecimenSearch.aspx")

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.NAME, COLLECTOR)))
    finally:
        driver.quit()


def create_tropicos_table(cxn, nfn):
    """Create Tropicos table."""
    nfn.to_sql('tropicos_data', cxn, if_exists='replace')

    sql = """CREATE UNIQUE INDEX IF NOT EXISTS
             tropicos_data_sample_id ON tropicos_data (sample_id)"""
    cxn.execute(sql)


if __name__ == '__main__':
    crawl_tropicos_website()
