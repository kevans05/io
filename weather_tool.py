from datetime import datetime
from urllib import request
from os import remove
from dateutil import rrule

import csv


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


    def read_one_month_from_gc(station, dt):
        url_part_1 = 'http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID='
        url_part_2 = '&Year='
        url_part_3 = '&Month='
        url_part_4 = '&Day=14&timeframe=1&submit=%20Download+Data'

        tmp_address_part_1 = 'tmp/'
        tmp_address_part_2 = '.csv'

        weather_data_dict = {'Date': datetime(1, 1, 1, 1, 1, 0), 'Temp_C': None,
                           'DewPointTemp_C': None, 'RelHum': None, 'StnPress_kPa': None, 'Hmdx': None}
        weather_data_hourly_list = []

        url = url_part_1 + str(station) + url_part_2 + str(dt.year) + url_part_3 + str(dt.month) + url_part_4
        tmp_address = tmp_address_part_1 + str(station) + str(dt.year) + str(dt.month) + tmp_address_part_2
        request.urlretrieve(url, tmp_address)

        with open(tmp_address) as weather_file:
            skip_header = 0
            while skip_header < 17:
                next(weather_file)
                skip_header = skip_header + 1
            reader = csv.reader(weather_file)
            for row in reader:
                hour = int(float(row[4].split(":")[0]))
                minute = int(float(row[4].split(":")[1]))
                dt = datetime(int(float(row[1])), int(float(row[2])), int(float(row[3])), hour, minute)
                weather_data_dict['Date'] = dt
                if len(row) == 25:
                    weather_data_dict['Temp_C'] = convert_str(row[6])
                    weather_data_dict['DewPointTemp_C'] = convert_str(row[8])
                    weather_data_dict['RelHum'] = convert_str(row[10])
                    weather_data_dict['StnPress_kPa'] = convert_str(row[18])
                    weather_data_dict['Hmdx'] = convert_str(row[20])
                weather_data_hourly_list.append(weather_data_dict.copy())
        remove(tmp_address)
        return weather_data_hourly_list

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


    def read_multiple_month_from_gc(station, dt_start, dt_end=None):
        if dt_end is None:
            dt_end = datetime.now()
        weather_data_hourly_list = []
        for dt in rrule.rrule(rrule.MONTHLY, dtstart=dt_start, until=dt_end):
            weather_data_hourly_list = weather_data_hourly_list + read_one_month_from_gc(station, dt)
        return weather_data_hourly_list


    #########################################################
    #  Method name: update_station_list
    #  Parameters: None
    #  Return: None
    #  Functionality: Downloads the Government of Canada's
    #                   station list
    #########################################################

    def update_station_list(self):
        url = 'ftp://client_climate@ftp.tor.ec.gc.ca/Pub/Get_More_Data_Plus_de_donnees/Station%20Inventory%20EN.csv'
        request.urlretrieve(url, 'end/stationInventory.csv')

