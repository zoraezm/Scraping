import pandas as pd
import numpy as np
import psycopg2
import urllib

connection = psycopg2.connect(user="postgres",password="mysecretpassword",host="localhost",port="5432",database="AirUni")
cursor = connection.cursor()
cursor.execute('select emiscode from school_portal where classrooms is null order by emiscode')

all_emiscodes = cursor.fetchall()

for emiscode in all_emiscodes:
    URL = "https://schoolportal.punjab.gov.pk/sed_census/list_of_emis_detail.aspx?emiscode="+str(emiscode[0])
    print(URL)
    try:
        tables = pd.read_html(URL)
        for index, row in tables[3].iterrows():
            # district, tehsil, school_phone, school_status, bldg_ownership, head_name, head_grade, head_phone, est_year, update_primary, update_middle, bldg_status,
            # const_type, bldg_condition, area_in_kanal, area_in_marla, covered_area_sqft
            if index>0:
                if row[0]=='Classrooms':
                    classrooms = str(row[1])
                    print('Classrooms : '+str(row[1]))
                    postgres_insert_query = """update school_portal set classrooms=%s where emiscode=%s"""
                    record_to_insert = (classrooms, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'No. of Books':
                    no_of_books = str(row[1])
                    print('No. of Books  : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set no_of_books=%s where emiscode=%s"""
                    record_to_insert = (no_of_books, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Sections':
                    sections = str(row[1])
                    print('Sections  : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set sections=%s where emiscode=%s"""
                    record_to_insert = (sections, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Computer Lab':
                    computer_lab = str(row[1])
                    print('Computer Lab : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set computer_lab=%s where emiscode=%s"""
                    record_to_insert = (computer_lab, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Library':
                    library = str(row[1])
                    print('Library : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set library=%s where emiscode=%s"""
                    record_to_insert = (library, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Comp. Students':
                    comp_students = str(row[1])
                    print('Comp. Students : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set comp_students=%s where emiscode=%s"""
                    record_to_insert = (comp_students, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
    except urllib.error.HTTPError as exception:
        print(str(emiscode[0])+'----------------->'+str(exception))
        postgres_insert_query = """update school_portal set classrooms=%s where emiscode=%s"""
        record_to_insert = (-999,emiscode[0])
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()