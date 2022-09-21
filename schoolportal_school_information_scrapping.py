import pandas as pd
import numpy as np
import psycopg2
import requests
import urllib

connection = psycopg2.connect(user="postgres",password="mysecretpassword",host="localhost",port="5432",database="AirUni")
cursor = connection.cursor()
cursor.execute('select emiscode from school_portal where district is null order by emiscode')

all_emiscodes = cursor.fetchall()

for emiscode in all_emiscodes:
    URL = "https://schoolportal.punjab.gov.pk/sed_census/list_of_emis_detail.aspx?emiscode="+str(emiscode[0])
    print(URL)
    try:
        tables = pd.read_html(URL)

        for index, row in tables[0].iterrows():
            # district, tehsil, school_phone, school_status, bldg_ownership, head_name, head_grade, head_phone, est_year, update_primary, update_middle, bldg_status,
            # const_type, bldg_condition, area_in_kanal, area_in_marla, covered_area_sqft
            district = ''
            tehsil_name = ''
            school_phone = ''
            school_status = ''
            bldg_ownership = ''
            head_name = ''
            head_grade = ''
            head_phone = ''
            est_year = ''
            update_primary = ''
            update_middle = ''
            bldg_status = ''
            const_type = ''
            bldg_condition = ''
            area_in_kanal = ''
            area_in_marla = ''
            covered_area_sqft = ''
            if index>0:
                if row[0]=='Emiscode':
                    print('Emiscode : '+str(row[1]))
                if row[2]=='School Name':
                    print('School Name : '+str(row[3]))
                if row[4]=='District':
                    district = str(row[5])
                    print('District : '+str(row[5]))
                    postgres_insert_query = """update school_portal set district=%s where emiscode=%s"""
                    record_to_insert = (district, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0]=='Tehsil':
                    tehsil_name = str(row[1])
                    print('Tehsil : '+str(row[1]))
                    postgres_insert_query = """update school_portal set tehsil=%s where emiscode=%s"""
                    record_to_insert = (tehsil_name, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[2]=='Level':
                    print('Level : '+str(row[3]))
                if row[4]=='Gender':
                    print('Gender : '+str(row[5]))
                if row[0]=='School Phone':
                    school_phone = row[1]
                    print('School Phone : '+str(row[1]))
                    postgres_insert_query = """update school_portal set school_phone=%s where emiscode=%s"""
                    record_to_insert = (school_phone, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[2]=='Mauza':
                    print('Mauza : '+str(row[3]))
                if row[4]=='School Status':
                    school_status = str(row[5])
                    print('School Status : '+str(row[5]))
                    postgres_insert_query = """update school_portal set school_status=%s where emiscode=%s"""
                    record_to_insert = (school_status, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0]=='Bldg Ownership':
                    bldg_ownership = str(row[1])
                    print('Bldg Ownership : '+str(row[1]))
                    postgres_insert_query = """update school_portal set bldg_ownership=%s where emiscode=%s"""
                    record_to_insert = (bldg_ownership, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[2]=='Head Name':
                    head_name = str(row[3])
                    print('Head Name : '+str(row[3]))
                    postgres_insert_query = """update school_portal set head_name=%s where emiscode=%s"""
                    record_to_insert = (head_name, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[4]=='Head Grade':
                    head_grade = str(row[5])
                    print('Head Grade : '+str(row[5]))
                    postgres_insert_query = """update school_portal set head_grade=%s where emiscode=%s"""
                    record_to_insert = (head_grade, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Head Phone':
                    head_phone = row[1]
                    print('Head Phone : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set head_phone=%s where emiscode=%s"""
                    record_to_insert = (head_phone, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[2] == 'Est. Year':
                    est_year = row[3]
                    print('Est. Year : ' + str(row[3]))
                    postgres_insert_query = """update school_portal set est_year=%s where emiscode=%s"""
                    record_to_insert = (est_year, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[4] == 'Update Primary':
                    update_primary = row[5]
                    print('Update Primary : ' + str(row[5]))
                    postgres_insert_query = """update school_portal set update_primary=%s where emiscode=%s"""
                    record_to_insert = (update_primary, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Update Middle':
                    update_middle = row[1]
                    print('Update Middle : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set update_middle=%s where emiscode=%s"""
                    record_to_insert = (update_middle, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[2] == 'Bldg. Status':
                    bldg_status = str(row[3])
                    print('Bldg. Status : ' + str(row[3]))
                    postgres_insert_query = """update school_portal set bldg_status=%s where emiscode=%s"""
                    record_to_insert = (bldg_status, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[4] == 'Const. Type':
                    const_type = str(row[5])
                    print('Const. Type : ' + str(row[5]))
                    postgres_insert_query = """update school_portal set const_type=%s where emiscode=%s"""
                    record_to_insert = (const_type, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Bldg. Condition':
                    bldg_condition = str(row[1])
                    print('Bldg. Condition : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set bldg_condition=%s where emiscode=%s"""
                    record_to_insert = (bldg_condition, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[2] == 'Area in Kanal':
                    area_in_kanal = row[3]
                    print('Area in Kanal : ' + str(row[3]))
                    postgres_insert_query = """update school_portal set area_in_kanal=%s where emiscode=%s"""
                    record_to_insert = (area_in_kanal, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[4] == 'Area in Marla':
                    area_in_marla = row[5]
                    print('Area in Marla : ' + str(row[5]))
                    postgres_insert_query = """update school_portal set area_in_marla=%s where emiscode=%s"""
                    record_to_insert = (area_in_marla, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Covered Area (sqft)':
                    covered_area_sqft = row[1]
                    print('Covered Area (sqft) : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set covered_area_sqft=%s where emiscode=%s"""
                    record_to_insert = (covered_area_sqft, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
    except urllib.error.HTTPError as exception:
        print(str(emiscode[0])+'----------------->'+str(exception))
        postgres_insert_query = """update school_portal set district=%s where emiscode=%s"""
        record_to_insert = ('tmp',emiscode[0])
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()