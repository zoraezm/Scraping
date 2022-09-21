import pandas as pd
import numpy as np
import psycopg2
import requests
import urllib

connection = psycopg2.connect(user="postgres",password="mysecretpassword",host="localhost",port="5432",database="AirUni")
cursor = connection.cursor()
cursor.execute('select emiscode from school_portal where teacher_flag is null order by emiscode')

all_emiscodes = cursor.fetchall()

# https://schoolportal.punjab.gov.pk/sed_census/list_of_emis_detail.aspx?emiscode=31240497
# all_emiscodes = [['31250029']]
for emiscode in all_emiscodes:  
    URL = "https://schoolportal.punjab.gov.pk/sed_census/list_of_emis_detail.aspx?emiscode="+str(emiscode[0])
    print(URL)
    try:
        tables = pd.read_html(URL)
        for table in tables:
            if str(table).find('Teacher Name')>-1:
                # print(table)
                for index, row in table.iterrows():
                    print(index,row)
                    teacher_name = row[0] # or row['Teacher Name']
                    designation = row[1]
                    scale = row[2]
                    joining_date = row[3]
                    degree_level = row[4]
                    qualification = row[5]
                    postgres_insert_query = """insert into school_portal_teachers_information(emiscode,teacher_name,designation,scale,joining_date,degree_level,qualification) values(%s,%s,%s,%s,%s,%s,%s)"""
                    record_to_insert = (emiscode[0],teacher_name,designation,scale,joining_date,degree_level,qualification)
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                    postgres_insert_query = """update school_portal set teacher_flag=%s where emiscode=%s"""
                    record_to_insert = ('done',emiscode[0])
                    cursor.execute(postgres_insert_query,record_to_insert)
                    connection.commit()
            else:
                print(str(emiscode[0]) + '----------------->' + str('not available'))
                postgres_insert_query = """update school_portal set teacher_flag=%s where emiscode=%s"""
                record_to_insert = ('not available', emiscode[0])
                cursor.execute(postgres_insert_query, record_to_insert)
                connection.commit()
    # https: // schoolportal.punjab.gov.pk / sed_census / list_of_emis_detail.aspx?emiscode = 31110023
    except urllib.error.HTTPError as exception:
        print(str(emiscode[0])+'----------------->'+str(exception))
        postgres_insert_query = """update school_portal set teacher_flag=%s where emiscode=%s"""
        record_to_insert = ('tmp', emiscode[0])
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()