import pandas as pd

from datetime import datetime
from urllib import request
from dateutil import rrule
from ftplib import FTP

import tempfile
import csv
import requests

#########################################################
#  Method name: convert_str
#  Parameters: s = the string that must be converted
#                       a string to a float and returns
#                       errors with 0 values
#  Return: a float from a string unless the value was na
#  Functionality: data is dirty and needs to be purified
#########################################################

def convert_str(s):
    try:
        ret = float(s)
    except ValueError:
        ret = None
    return ret

class canadian_weather:
    def __init__(self):
        self.data_set_url_1 = 'http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID='
        self.data_set_url_2 = '&Year='
        self.data_set_url_3 = '&Month='
        self.data_set_url_4 = '&Day=14&timeframe=1&submit=%20Download+Data'

        self.weather_station_id_url = 'ftp://client_climate@ftp.tor.ec.gc.ca/Pub/Get_More_Data_Plus_de_donnees/Station%20Inventory%20EN.csv'



    #########################################################
    #  Method name: read_one_month_from_gc
    #  Parameters: station = this is the stationID that the
    #                            Government of Canada has
    #                            assigned to the specific
    #                            weather station
    #               dt = the date time that the function
    #                            is to download. this just
    #                            needs to include year and
    #                            month
    #  Return: a list of dictionaries that have weather data
    #                            in it
    #  Functionality: this will scrape the Government of
    #                 Canada's Weather Data base for the
    #                 specified station at a specific time
    #########################################################

    def read_one_month_from_gc(self, station, dt):
        tmp_address_part_1 = 'tmp/'
        tmp_address_part_2 = '.csv'

        dfObj = pd.DataFrame(columns=['datetime', 'Temp_C','DewPointTemp_C','RelHum','StnPress_kPa','Hmdx'])

        url = self.data_set_url_1 + str(station) + self.data_set_url_2 + str(dt.year) + self.data_set_url_3 + str(dt.month) + self.data_set_url_4

        temp = tempfile.NamedTemporaryFile()

        try:
            response = requests.get(url)
            with open(temp.name, 'wb') as file:
                file.write(response.content)
            with open(temp.name) as weather_file:
                skip_header = 0
                while skip_header < 17:
                    next(weather_file)
                    skip_header = skip_header + 1
                reader = csv.reader(weather_file)
                for row in reader:
                    dt = datetime(int(float(row[1])), int(float(row[2])), int(float(row[3])),
                                  int(float(row[4].split(":")[0])), int(float(row[4].split(":")[1])))
                    if len(row) == 24:
                        if convert_str(row[5]) is not None:
                            dfObj = dfObj.append({'datetime': dt, 'Temp_C':convert_str(row[5]),
                                                  'DewPointTemp_C':convert_str(row[7]),'RelHum':convert_str(row[9]),
                                                  'StnPress_kPa':convert_str(row[17]),'Hmdx':convert_str(row[19])
                                                  },ignore_index = True)
        finally:
            temp.close()
        return dfObj

    #########################################################
    #  Method name: read_multiple_month_from_gc
    #  Parameters: station = this is the stationID that the
    #                            Government of Canada has
    #                            assigned to the specific
    #                            weather station
    #               dt_start = the first date of the period
    #                            that the function is to
    #                            download. this just needs
    #                            to include year and
    #                            month
    #               dt_end = the last date of the period
    #                            that the function is to
    #                            download. this just needs
    #                            to include year and
    #                            month default will be today
    #  Return: a list of dictionaries that have weather data
    #                            in it for the defined period
    #  Functionality: this will scrape the Government of
    #                 Canada's Weather Data base for the
    #                 specified station at a specific time
    #########################################################

    def read_multiple_month_from_gc(self, station, dt_start, dt_end=None):
        if dt_end is None:
            dt_end = datetime.now()
        weather_data_hourly_list = []
        for dt in rrule.rrule(rrule.MONTHLY, dtstart=dt_start, until=dt_end):
            weather_data_hourly_list = weather_data_hourly_list + self.read_one_month_from_gc(station, dt)
        return weather_data_hourly_list

def download_ontario_data():
    ontario_weather = canadian_weather()
    weather_station = request.urlretrieve(ontario_weather.weather_station_id_url, 'stationInventory.csv')

download_ontario_data()


    #print(ontario_weather.read_one_month_from_gc(10999,datetime.now()))