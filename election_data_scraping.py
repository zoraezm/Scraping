import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib.request
import psycopg2
import warnings

warnings.filterwarnings("ignore")
connection = psycopg2.connect(user="postgres",password="mysecretpassword",host="localhost",port="5433",database="postgres")
cursor = connection.cursor()


# URL = 'https://schoolportal.punjab.gov.pk/sed_census/new_emis_details.aspx?distId=371--Attock'
URL = 'https://na.gov.pk/en/all_members.php'

page = requests.get(URL, verify=False)
soup = BeautifulSoup(page.content, 'html.parser')
data = {}
full_data = []
lst_mna_small_profile_pics = []
lst_mna_big_profile_pics = []
lst_mna_names = []
lst_mna_detail_pages = []
lst_mna_details = []
# df = pd.DataFrame(data, columns=['MNA Name', 'Father Name', 'Permanent Address', 'Local Address', 'Contact Number',
#                                  'Province', 'Constituency', 'Party', 'Oath Taking Day'])

# table = soup.find("table", id="mytable12 table-bordered table-hover")

table = soup.find("table", {"class": "mytable12 table-bordered table-hover"})
rows = table.find_all("tr")


# -------- Code to get Low Resolution Images from URL ---------------
mna_profiles = soup.find_all("td", id="mna_profile")
for mna_profile in mna_profiles:
    for img in mna_profile.find_all('img', alt=True):
        tmp_url = 'https://na.gov.pk/PhpThumb/phpThumb.php?h=80&q=50&src=../uploads'+img['src'].split('src=../uploads')[1]
        print(tmp_url)
        lst_mna_small_profile_pics.append(tmp_url)

# -------- Code to get High Resolution Images from URL ---------------
# img = table.find_all('img')
# raw_image_url = img[0]['src']
# print(mna_big_pics.text)
# image_url = 'https://na.gov.pk' + str(raw_image_url).split('..')[1]
# image_name = raw_image_url.split('../uploads/images/')[1]
# r = requests.get(image_url, allow_redirects=True)
# open('mna_big_pics/' + mna_big_pics.text + ".jpg", 'wb').write(r.content)
# --------------------------------------------------------------------


#----------       Code to get all MNA names and their urls
mna_names = soup.find_all("td", id="mna_big_pics")
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


for mna_name,small_pic_url,big_pic_url,mna_details,mna_url in zip(lst_mna_names,lst_mna_small_profile_pics,lst_mna_big_profile_pics,lst_mna_details, lst_mna_detail_pages):
    mna_father_name = ''
    mna_permanent_address = ''
    mna_local_address = ''
    mna_contact_numbers = ''
    mna_province = ''
    mna_constituency = ''
    mna_party = ''
    mna_oath_taking_date = ''
    for key, value in mna_details.items():
        # print(key, '->', value)

        if key=='Name':
            mna_name = value
        elif key == "Father's Name":
            mna_father_name = value
        elif key == 'Permanent Address':
            mna_permanent_address = value
        elif key == 'Local Address':
            mna_local_address = value
        elif key == 'Contact Number':
            mna_contact_numbers = value
        elif key == 'Province':
            mna_province = value
        elif key == 'Constituency':
            mna_constituency = value
        elif key == 'Party':
            mna_party = value
        elif key == 'Oath Taking Date':
            mna_oath_taking_date = value

    postgres_insert_query = """insert into mna_details(mna_name,father_name,permanent_address,local_address,contact_number,province,constituency,party,oath_taking_date,mna_url,profile_pic_big_url,profile_pic_small_url) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    record_to_insert = (mna_name,mna_father_name,mna_permanent_address,mna_local_address,mna_contact_numbers,mna_province,mna_constituency,mna_party,mna_oath_taking_date,mna_url,big_pic_url,small_pic_url)
    print(record_to_insert)
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()

    # data['header'] = table_data

    # rows = table.find_all('tr')
    # for row in rows:
    #     cols1 = row.find_all('th')
    #     cols1 = [ele.text.strip() for ele in cols1]
    #
    #     cols = row.find_all('td')
    #     cols = [ele.text.strip() for ele in cols]
    #     data.append([ele for ele in cols if ele])  # Get rid of empty values



# mna_profiles = soup.find_all("td", id="mna_profile")

# for mna_big_pics in mna_names:
#     mna_url = 'https://na.gov.pk/en/' + mna_big_pics.a['href']
#     print(mna_url)
#     mna_details = requests.get(mna_url, verify=False)
#     soup = BeautifulSoup(mna_details.content, 'html.parser')
#     table = soup.find("table", {"class": "profile_tbl table-bordered"})
#
#     img = table.find_all('img')
#     raw_image_url = img[0]['src']
#     print(mna_big_pics.text)
#     image_url = 'https://na.gov.pk' + str(raw_image_url).split('..')[1]
#
#     # table_body = table.find('tbody')
#
#     rows = table.find_all('tr')
#     for row in rows:
#         cols1 = row.find_all('th')
#         cols1 = [ele.text.strip() for ele in cols1]
#
#         cols = row.find_all('td')
#         cols = [ele.text.strip() for ele in cols]
#         data.append([ele for ele in cols if ele])  # Get rid of empty values
#
#     full_data.append(data)
#     # data = []
#
#     print(len(data))
#     try:
#         mna_name = data[0][0]
#         mna_father_name = data[1][0]
#         mna_permanent_address = data[2][0]
#         mna_local_address = data[3][0]
#         mna_contact_numbers = data[4][0]
#         mna_province = data[5][0]
#         mna_constituency = data[6][0]
#         mna_party = data[7][0]
#         mna_oath_taking_date = data[8][0]
#
#
#     except IndexError:
#         pass
#     # postgres_insert_query = """insert into mna_details(mna_name,father_name,permanent_address,local_address,contact_number,province,constituency,party,oath_taking_date,mna_url,profile_pic_url) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
#     # record_to_insert = (mna_name,mna_father_name,mna_permanent_address,mna_local_address,mna_contact_numbers,mna_province,mna_constituency,mna_party,mna_oath_taking_date,mna_url,image_url)
#     # cursor.execute(postgres_insert_query, record_to_insert)
#     # connection.commit()
#
#     data = []
