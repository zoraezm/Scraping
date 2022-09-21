import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib.request

# URL = 'https://schoolportal.punjab.gov.pk/sed_census/new_emis_details.aspx?distId=371--Attock'
URL = 'https://na.gov.pk/en/all_members.php'

page = requests.get(URL, verify=False)
soup = BeautifulSoup(page.content, 'html.parser')


data = []
lst_mna_details = []
lst_mna_detail_pages = []
lst_mna_names = []
lst_mna_big_profile_pics = []
lst_mna_small_profile_pics = []

table = soup.find("table",{"class":"mytable12 table-bordered table-hover"})

mna_profiles = soup.find_all("td", id="mna_profile")

for mna_profile in mna_profiles:
    for img in mna_profile.find_all('img', alt=True):
        tmp_url = 'https://na.gov.pk/PhpThumb/phpThumb.php?h=80&q=50&src=../uploads'+img['src'].split('src=../uploads')[1]
        print(tmp_url)
        lst_mna_small_profile_pics.append(tmp_url)

mna_names = table.find_all("td", id="mna")
for mna_name in mna_names:
    # print(mna_name.text)
    # print('https://na.gov.pk/en/'+mna_name.a['href'])
    lst_mna_names.append(mna_name.text)
    lst_mna_detail_pages.append('https://na.gov.pk/en/'+mna_name.a['href'])

for mna_url in lst_mna_detail_pages:
    mna_details = requests.get(mna_url, verify=False)
    soup = BeautifulSoup(mna_details.content, 'html.parser')
    table = soup.find("table", {"class": "profile_tbl table-bordered"})

    img = table.find_all('img')
    raw_image_url = img[0]['src']
    image_url = 'https://na.gov.pk' + str(raw_image_url).split('..')[1]
    lst_mna_big_profile_pics.append(image_url)
    # print(image_url)


    headings = []
    for td in table.find_all("td"):
        headings.append(td.text.replace('\n', ' ').strip())


    t_headers = []
    t_row_tmp = []
    t_row = {}
    for th in table.find_all("th"):
        # print(th.text)
        t_headers.append(th.text.replace('\n', ' ').strip())
    table_data = []
    for tr in table.find_all("tr"):
        for td in tr.find_all("td"):
            t_row_tmp.append(td.text.replace('\n', ' ').strip())

    t_row_tmp.pop(1)
    for th,td in zip(t_headers,t_row_tmp):
        # print(th,td)
        t_row[th] = td
    lst_mna_details.append(t_row)


for mna_name,small_profile_pic,big_profile_pic,mna_detail in zip(lst_mna_names,lst_mna_small_profile_pics,lst_mna_big_profile_pics,lst_mna_details):
    print(mna_name)
    print(big_profile_pic)
    print(mna_detail)
    try:
        const_id = str(str(mna_detail['Constituency']).split('-')[1]).split('(')[0]
        # r = requests.get(big_profile_pic, allow_redirects=True)
        # open('mna_big_pics/' + const_id + ".jpg", 'wb').write(r.content)
        r = requests.get(small_profile_pic, allow_redirects=True)
        open('mna_small_pics/' + const_id + ".jpg", 'wb').write(r.content)
    except KeyError:
        pass