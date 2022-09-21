import pandas as pd
import numpy as np
import psycopg2
import urllib

connection = psycopg2.connect(user="postgres",password="mysecretpassword",host="localhost",port="5432",database="AirUni")
cursor = connection.cursor()
cursor.execute('select emiscode from school_portal where hockey is null order by emiscode')

all_emiscodes = cursor.fetchall()

for emiscode in all_emiscodes:
    URL = "https://schoolportal.punjab.gov.pk/sed_census/list_of_emis_detail.aspx?emiscode="+str(emiscode[0])
    print(URL)
    try:
        tables = pd.read_html(URL)
        for index, row in tables[2].iterrows():
            # district, tehsil, school_phone, school_status, bldg_ownership, head_name, head_grade, head_phone, est_year, update_primary, update_middle, bldg_status,
            # const_type, bldg_condition, area_in_kanal, area_in_marla, covered_area_sqft
            if index>0:
                if row[0]=='Hockey':
                    hockey = str(row[1])
                    print('Hockey : '+str(row[1]))
                    postgres_insert_query = """update school_portal set hockey=%s where emiscode=%s"""
                    record_to_insert = (hockey, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Table Tennis':
                    table_tennis = str(row[1])
                    print('Table Tennis  : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set table_tennis=%s where emiscode=%s"""
                    record_to_insert = (table_tennis, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Cricket':
                    cricket = str(row[1])
                    print('Cricket  : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set cricket=%s where emiscode=%s"""
                    record_to_insert = (cricket, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Badminton':
                    badminton = str(row[1])
                    print('Badminton : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set badminton=%s where emiscode=%s"""
                    record_to_insert = (badminton, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Football':
                    football = str(row[1])
                    print('Football : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set football=%s where emiscode=%s"""
                    record_to_insert = (football, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Volley Ball':
                    volley_ball = str(row[1])
                    print('Volley Ball : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set volley_ball=%s where emiscode=%s"""
                    record_to_insert = (volley_ball, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
    except urllib.error.HTTPError as exception:
        print(str(emiscode[0])+'----------------->'+str(exception))
        postgres_insert_query = """update school_portal set hockey=%s where emiscode=%s"""
        record_to_insert = ('tmp',emiscode[0])
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()