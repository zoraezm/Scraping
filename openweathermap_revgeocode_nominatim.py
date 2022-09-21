from time import sleep
from random import randint
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import logging
import psycopg2
import re

connection = psycopg2.connect(user="postgres",password="mysecretpassword",host="localhost",port="5433",database="postgres")
cursor = connection.cursor()

def update_nominatim(cursor, table_name, geolocator):
    sql_stmt = 'select id_0,old_lat,old_lon from '+table_name+" where population>500 and osm_status='false'";
    cursor.execute(sql_stmt)
    res = cursor.fetchall()
    for r in res:
        nom_res = reverse_geocode(geolocator, str(r[1])+','+str(r[2]), 1)
        if nom_res is None:
            print('Nominatim has None Response')
            update_sql_stmt = 'UPDATE ' + table_name + " SET osm_status='true' WHERE id_0 = " + str(
                r[0])
            cursor.execute(update_sql_stmt)
            connection.commit()
            pass
        else:
            # nom_res = reverse_geocode(geolocator, '33.67127852854555, 73.14085271154654', 1)
            var_osm_road = ''
            var_osm_village = ''
            var_osm_city = ''
            var_osm_postcode = ''
            var_osm_state = ''
            var_osm_country = ''

            if 'country' not in nom_res.raw['address'].keys():
                var_osm_country = ''
                pass
                # print('Country is not available')
            else:
                var_osm_country = re.sub("[$@&']","",nom_res.raw['address']['country'])

            if 'state' not in nom_res.raw['address'].keys():
                var_osm_state = ''
                pass
                # print('State is not available')
            else:
                var_osm_state = re.sub("[$@&']","",nom_res.raw['address']['state'])
            if 'road' not in nom_res.raw['address'].keys():
                var_osm_road = ''
                pass
                # print('Road is not available')
            else:
                var_osm_road = re.sub("[$@&']","",nom_res.raw['address']['road'])
            if 'city' not in nom_res.raw['address'].keys():
                # print('City is not available')
                var_osm_city = ''
                pass
            else:
                var_osm_city = nom_res.raw['address']['city']
            if 'village' not in nom_res.raw['address'].keys():
                # print('Village is not available')
                var_osm_village = ''
                pass
            else:
                var_osm_village = re.sub("[$@&']","",nom_res.raw['address']['village'])
            if 'postcode' not in nom_res.raw['address'].keys():
                # print('Village is not available')
                var_osm_postcode = ''
                pass
            else:
                var_osm_postcode = nom_res.raw['address']['postcode']


            # for key in nom_res.raw['address'].keys():
            #     print(key)
            var_display_name =  re.sub("[$@&']","",nom_res.raw['display_name'])
            update_sql_stmt = 'UPDATE '+table_name+ " SET osm_display_name='"+var_display_name+"', osm_osm_id="+str(nom_res.raw['osm_id'])+",osm_place_id="+str(nom_res.raw['place_id'])+",osm_osm_type='"+str(nom_res.raw['osm_type'])+"',osm_country='"+var_osm_country+"',osm_state='"+var_osm_state+"',osm_country_code='"+str(nom_res.raw['address']['country_code'])+"',osm_postcode='"+var_osm_postcode+"',osm_village='"+var_osm_village+"',osm_road='"+var_osm_road+"',osm_city='"+var_osm_city+"', osm_status='true' WHERE id_0 = "+str(r[0])
            cursor.execute(update_sql_stmt)
            connection.commit()
            print(update_sql_stmt)
            # print('\n')

def reverse_geocode(geolocator, latlon, sleep_sec):
    try:
        return geolocator.reverse(latlon)
    except GeocoderTimedOut:
        logging.info('TIMED OUT: GeocoderTimedOut: Retrying...')
        # sleep(randint(1*100,sleep_sec*100)/100)
        return reverse_geocode(geolocator, latlon, sleep_sec)
    except GeocoderServiceError as e:
        logging.info('CONNECTION REFUSED: GeocoderServiceError encountered.')
        logging.error(e)
        return None
    except Exception as e:
        logging.info('ERROR: Terminating due to exception {}'.format(e))
        return None

user_agent = 'user_me_{}'.format(randint(10000,99999))
geolocator = Nominatim(user_agent=user_agent)


update_nominatim(cursor, 'weather_openweathermapapi', geolocator)

# osm_id, osm_type, place_id display_name,
# address.country, country_code, postcode, road, state, village, state