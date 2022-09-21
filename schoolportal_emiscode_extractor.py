import pandas as pd
import psycopg2
import requests
from bs4 import BeautifulSoup



districts_url_lst = ['https://schoolportal.punjab.gov.pk/sed_census/new_emis_details.aspx?distId=371--Attock']

for URL in districts_url_lst:
    page = requests.get(URL, verify=False)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find("table", id="main1_grd_emis_details")
    abc = pd.read_html(table.text)
    rows = table.find_all("tr")
    emis_codes = []
    emis_urls = []
    for row in rows:
        tds = row.find_all("td")
        if len(tds) > 0:
            for td in tds:
                emis_codes.append(td.text)
            # emis_urls.append(td.a['href'])

    tables = pd.read_html(table)
    print(tables)