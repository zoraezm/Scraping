import pandas as pd
import numpy as np
import psycopg2
import urllib

connection = psycopg2.connect(user="postgres",password="mysecretpassword",host="localhost",port="5432",database="AirUni")
cursor = connection.cursor()
cursor.execute('select emiscode from school_portal where drink_water is null order by emiscode')

all_emiscodes = cursor.fetchall()

for emiscode in all_emiscodes:
    URL = "https://schoolportal.punjab.gov.pk/sed_census/list_of_emis_detail.aspx?emiscode="+str(emiscode[0])
    print(URL)
    try:
        tables = pd.read_html(URL)
        for index, row in tables[1].iterrows():
            # district, tehsil, school_phone, school_status, bldg_ownership, head_name, head_grade, head_phone, est_year, update_primary, update_middle, bldg_status,
            # const_type, bldg_condition, area_in_kanal, area_in_marla, covered_area_sqft
            if index>0:
                if row[0]=='Drink Water':
                    drink_water = str(row[1])
                    print('Drink Water : '+str(row[1]))
                    postgres_insert_query = """update school_portal set drink_water=%s where emiscode=%s"""
                    record_to_insert = (drink_water, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Electricity':
                    electricity = str(row[1])
                    print('Electricity  : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set electricity=%s where emiscode=%s"""
                    record_to_insert = (electricity, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Boundary Wall':
                    boundary_wall = str(row[1])
                    print('Boundary Wall  : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set boundary_wall=%s where emiscode=%s"""
                    record_to_insert = (boundary_wall, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Main Gate':
                    main_gate = str(row[1])
                    print('Main Gate : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set main_gate=%s where emiscode=%s"""
                    record_to_insert = (main_gate, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Play Ground':
                    play_ground = str(row[1])
                    print('Play Ground : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set play_ground=%s where emiscode=%s"""
                    record_to_insert = (play_ground, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Toilets':
                    toilets = str(row[1])
                    print('Toilets : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set toilets=%s where emiscode=%s"""
                    record_to_insert = (toilets, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Teacher Furniture':
                    teacher_furniture = row[1]
                    print('Teacher Furniture : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set teacher_furniture=%s where emiscode=%s"""
                    record_to_insert = (teacher_furniture, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
                if row[0] == 'Student Furniture':
                    student_furniture = row[1]
                    print('Student Furniture : ' + str(row[1]))
                    postgres_insert_query = """update school_portal set student_furniture=%s where emiscode=%s"""
                    record_to_insert = (student_furniture, emiscode[0])
                    cursor.execute(postgres_insert_query, record_to_insert)
                    connection.commit()
    except urllib.error.HTTPError as exception:
        print(str(emiscode[0])+'----------------->'+str(exception))
        postgres_insert_query = """update school_portal set drink_water=%s where emiscode=%s"""
        record_to_insert = ('tmp',emiscode[0])
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()