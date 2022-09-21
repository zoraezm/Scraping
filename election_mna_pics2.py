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

# mna_names = soup.find_all("td", id="mna_big_pics")
# mna_profiles = soup.find_all("td", id="mna_profile")
lst_mna_small_profile_pics = []
lst_mna_names = []

mna_profiles = soup.find_all("td", id="mna_profile")
mna_names = soup.find_all("td", id="mna")

for mna_profile in mna_profiles:
    for img in mna_profile.find_all('img', alt=True):
        tmp_url = 'https://na.gov.pk/PhpThumb/phpThumb.php?h=80&q=50&src=../uploads'+img['src'].split('src=../uploads')[1]
        # print(tmp_url)
        lst_mna_small_profile_pics.append(tmp_url)

for mna_name in mna_names:
    # print(mna_name.text)
    lst_mna_names.append(mna_name.text)


for mna_name,image_url in zip(lst_mna_names,lst_mna_small_profile_pics):
    print(mna_name,image_url)
    r = requests.get(image_url, allow_redirects=True)
    open('mna_small_pics/' + mna_name+ ".jpg", 'wb').write(r.content)


    # open('mna_big_pics/'+mna.text+".jpg", 'wb').write(r.content)