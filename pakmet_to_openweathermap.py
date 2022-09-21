import requests
import psycopg2
from datetime import *
import json
# from threading import Thread
# from queue import Queue
import multiprocessing as mp

connection = psycopg2.connect(user="postgres",password="mysecretpassword",host="localhost",port="5433",database="postgres")
cursor = connection.cursor()

def update_pg_table(cursor, table_name, id_min, id_max):
    print(id_min,id_max)
    sql_stmt = 'select id,lat,lon from '+table_name+" where status='false' and id3>="+str(id_min)+' and id3<='+str(id_max);
    # print(sql_stmt)
    cursor.execute(sql_stmt)
    res = cursor.fetchall()
    for r in res:
        owpa_res = get_data_from_openweathermapapi(str(r[1]),str(r[2]))
        # print(owpa_res)
        update_sql_stmt = 'UPDATE '+table_name+ " SET lat1="+str(owpa_res['lat'])+", lon="+str(owpa_res['lon'])+",weather_main='"+owpa_res['weather_main']+"',weather_description='"+owpa_res['weather_description']+"',temperature="+str(owpa_res['temperature'])+",feels_like="+str(owpa_res['feels_like'])+",temp_min="+str(owpa_res['temp_min'])+",temp_max="+str(owpa_res['temp_max'])+",pressure="+str(owpa_res['pressure'])+",humidity="+str(owpa_res['humidity'])+",wind_speed="+str(owpa_res['wind_speed'])+",wind_degree="+str(owpa_res['wind_deg'])+",clouds="+str(owpa_res['clouds'])+",datetime='"+str(datetime.fromtimestamp(owpa_res['dt']))+"',city_name='"+owpa_res['name']+"',status='true' WHERE id = "+str(r[0])
        cursor.execute(update_sql_stmt)
        connection.commit()

def get_data_from_openweathermapapi(lat, lon):
    api_key = ''
    weather_response = {}
    data = requests.get('http://api.openweathermap.org/data/2.5/weather?lat=' + lat + '&lon=' + lon + '&appid='+api_key)
    dict2 = json.loads(data.text)
    weather_response['lat'] = dict2['coord']['lat']
    weather_response['lon'] = dict2['coord']['lon']
    weather_response['weather_main'] = dict2['weather'][0]['main']
    weather_response['weather_description'] = dict2['weather'][0]['description']
    weather_response['temperature'] = dict2['main']['temp']
    weather_response['feels_like'] = dict2['main']['feels_like']
    weather_response['temp_min'] = dict2['main']['temp_min']
    weather_response['temp_max'] = dict2['main']['temp_max']
    weather_response['pressure'] = dict2['main']['pressure']
    weather_response['humidity'] = dict2['main']['humidity']
    weather_response['wind_speed'] = dict2['wind']['speed']
    weather_response['wind_deg'] = dict2['wind']['deg']
    weather_response['clouds'] = dict2['clouds']['all']
    weather_response['dt'] = dict2['dt']
    weather_response['name'] = dict2['name']
    return weather_response

t1 = mp.Process(target=update_pg_table(cursor, 'weather_pakmet_openweather', 1, 2079), name='t1')
t2 = mp.Process(target=update_pg_table(cursor, 'weather_pakmet_openweather', 2080, 4159), name='t2')
t3 = mp.Process(target=update_pg_table(cursor, 'weather_pakmet_openweather', 4160, 6239), name='t3')
t4 = mp.Process(target=update_pg_table(cursor, 'weather_pakmet_openweather', 6240, 8320), name='t4')


t1.start()
t2.start()
t3.start()
t4.start()

t1.join()
t2.join()
t3.join()
t4.join()

# osm_id, osm_type, place_id display_name,
# address.country, country_code, postcode, road, state, village, state