import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib.request

# URL = 'https://schoolportal.punjab.gov.pk/sed_census/new_emis_details.aspx?distId=371--Attock'
URL = 'https://na.gov.pk/en/all_members.php'

page = requests.get(URL, verify=False)
soup = BeautifulSoup(page.content, 'html.parser')
data = []
# table = soup.find("table", id="mytable12 table-bordered table-hover")

table = soup.find("table",{"class":"mytable12 table-bordered table-hover"})
rows = table.find_all("tr")

mna_names = soup.find_all("td", id="mna_big_pics")
# mna_profiles = soup.find_all("td", id="mna_profile")

for mna in mna_names:
    mna_url = 'https://na.gov.pk/en/'+mna.a['href']
    print(mna_url)
    mna_details = requests.get(mna_url, verify=False)
    soup = BeautifulSoup(mna_details.content, 'html.parser')
    table_div = soup.find("table",{"class":"profile_tbl table-bordered"})



    #-------- Code to get High Resolution Images from URL ---------------
    img = table_div.find_all('img')
    raw_image_url = img[0]['src']
    print(mna.text)
    image_url = 'https://na.gov.pk'+str(raw_image_url).split('..')[1]
    image_name = raw_image_url.split('../uploads/images/')[1]
    r = requests.get(image_url, allow_redirects=True)
    open('mna_big_pics/'+mna.text+".jpg", 'wb').write(r.content)
    #--------------------------------------------------------------------
