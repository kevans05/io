import pandas as pd

from datetime import datetime
from urllib import request
from dateutil import rrule
from ftplib import FTP

import tempfile
import csv
import requests

weather_station = [{"Station ID":"52918","Name":"ATTAWAPISKAT A"},
                    {"Station ID":"49488","Name":"BIG TROUT LAKE A"},
                    {"Station ID":"50857","Name":"BIG TROUT LAKE"},
                    {"Station ID":"27865","Name":"EAR FALLS (AUT)"},
                    {"Station ID":"51219","Name":"FORT SEVERN A"},
                    {"Station ID":"51237","Name":"LANSDOWNE HOUSE A"},
                    {"Station ID":"10244","Name":"LANSDOWNE HOUSE (AUT)"},
                    {"Station ID":"50722","Name":"MUSKRAT DAM"},
                    {"Station ID":"10197","Name":"PEAWANUCK (AUT)"},
                    {"Station ID":"52878","Name":"PEAWANUCK A"},
                    {"Station ID":"3920","Name":"PICKLE LAKE (AUT)"},
                    {"Station ID":"52418","Name":"PICKLE LAKE A"},
                    {"Station ID":"50478","Name":"PICKLE LAKE A"},
                    {"Station ID":"50248","Name":"RED LAKE A"},
                    {"Station ID":"52419","Name":"RED LAKE A"},
                    {"Station ID":"50723","Name":"SANDY LAKE A"},
                    {"Station ID":"3932","Name":"BARWICK"},
                    {"Station ID":"10220","Name":"ATIKOKAN (AUT)"},
                    {"Station ID":"46507","Name":"FORT FRANCES RCS"},
                    {"Station ID":"44343","Name":"MINE CENTRE SOUTHWEST"},
                    {"Station ID":"48869","Name":"DRYDEN REGIONAL"},
                    {"Station ID":"47307","Name":"KENORA RCS"},
                    {"Station ID":"51137","Name":"KENORA A"},
                    {"Station ID":"3966","Name":"RAWSON LAKE"},
                    {"Station ID":"30455","Name":"RAWSON LAKE (AUT)"},
                    {"Station ID":"10899","Name":"ROYAL ISLAND (AUT)"},
                    {"Station ID":"51138","Name":"SIOUX LOOKOUT A"},
                    {"Station ID":"54605","Name":"SIOUX LOOKOUT AIRPORT"},
                    {"Station ID":"3987","Name":"ARMSTRONG (AUT)"},
                    {"Station ID":"52900","Name":"ARMSTRONG A"},
                    {"Station ID":"27674","Name":"CAMERON FALLS (AUT)"},
                    {"Station ID":"7582","Name":"CARIBOU ISLAND (AUT)"},
                    {"Station ID":"53519","Name":"GERALDTON A"},
                    {"Station ID":"54240","Name":"GERALDTON A"},
                    {"Station ID":"54858","Name":"GERALDTON AIRPORT"},
                    {"Station ID":"54378","Name":"HAZELWOOD"},
                    {"Station ID":"52338","Name":"MARATHON A"},
                    {"Station ID":"53138","Name":"MARATHON A"},
                    {"Station ID":"49848","Name":"PUKASKWA (AUT)"},
                    {"Station ID":"54606","Name":"TERRACE BAY AIRPORT"},
                    {"Station ID":"49389","Name":"THUNDER BAY"},
                    {"Station ID":"50132","Name":"THUNDER BAY A"},
                    {"Station ID":"44324","Name":"THUNDER BAY BURWOOD"},
                    {"Station ID":"30682","Name":"THUNDER BAY CS"},
                    {"Station ID":"4057","Name":"UPSALA (AUT)"},
                    {"Station ID":"4061","Name":"WELCOME ISLAND (AUT)"},
                    {"Station ID":"43104","Name":"LITTLE FLATLAND ISLAND (AUT)"},
                    {"Station ID":"26912","Name":"NORTHERN ONTARIO EER"},
                    {"Station ID":"51958","Name":"LAKE SUPERIOR PROVINCIAL PARK"},
                    {"Station ID":"50092","Name":"SAULT STE MARIE A"},
                    {"Station ID":"54838","Name":"SAULT STE. MARIE AIRPORT"},
                    {"Station ID":"52818","Name":"WAWA A"},
                    {"Station ID":"52998","Name":"WAWA A"},
                    {"Station ID":"8997","Name":"KILLARNEY (AUT)"},
                    {"Station ID":"53602","Name":"CHAPLEAU A"},
                    {"Station ID":"53603","Name":"CHAPLEAU A"},
                    {"Station ID":"4121","Name":"MASSEY"},
                    {"Station ID":"53618","Name":"MONETVILLE 2"},
                    {"Station ID":"49508","Name":"SUDBURY CLIMATE"},
                    {"Station ID":"50840","Name":"SUDBURY A"},
                    {"Station ID":"4140","Name":"BONNER LAKE"},
                    {"Station ID":"49489","Name":"EARLTON A"},
                    {"Station ID":"47687","Name":"EARLTON CLIMATE"},
                    {"Station ID":"52604","Name":"KAPUSKASING A"},
                    {"Station ID":"54260","Name":"KAPUSKASING A"},
                    {"Station ID":"30435","Name":"KAPUSKASING CDA ON"},
                    {"Station ID":"27535","Name":"KIRKLAND LAKE CS"},
                    {"Station ID":"48950","Name":"MOOSONEE"},
                    {"Station ID":"4168","Name":"MOOSONEE UA"},
                    {"Station ID":"42123","Name":"MOOSONEE RCS"},
                    {"Station ID":"7633","Name":"NAGAGAMI (AUT)"},
                    {"Station ID":"52898","Name":"OGOKI POST A"},
                    {"Station ID":"47547","Name":"TIMMINS CLIMATE"},
                    {"Station ID":"50460","Name":"TIMMINS A"},
                    {"Station ID":"42967","Name":"ALGONQUIN PARK EAST GATE"},
                    {"Station ID":"52318","Name":"NORTH BAY A"},
                    {"Station ID":"54604","Name":"NORTH BAY AIRPORT"},
                    {"Station ID":"50839","Name":"NORTH BAY A"},
                    {"Station ID":"48372","Name":"GORE BAY-MANITOULIN"},
                    {"Station ID":"48788","Name":"GORE BAY CLIMATE"},
                    {"Station ID":"7981","Name":"GREAT DUCK ISLAND (AUT)"},
                    {"Station ID":"6901","Name":"APPLETON"},
                    {"Station ID":"47567","Name":"BROCKVILLE CLIMATE"},
                    {"Station ID":"4236","Name":"BROCKVILLE PCC"},
                    {"Station ID":"4243","Name":"CHALK RIVER AECL"},
                    {"Station ID":"4255","Name":"CORNWALL"},
                    {"Station ID":"4268","Name":"DRUMMOND CENTRE"},
                    {"Station ID":"10903","Name":"GRENADIER ISLAND"},
                    {"Station ID":"4287","Name":"HARTINGTON IHD"},
                    {"Station ID":"27534","Name":"KEMPTVILLE CS"},
                    {"Station ID":"47267","Name":"KINGSTON CLIMATE"},
                    {"Station ID":"50428","Name":"KINGSTON A"},
                    {"Station ID":"52985","Name":"KINGSTON A"},
                    {"Station ID":"4308","Name":"LYNDHURST SHAWMERE"},
                    {"Station ID":"41738","Name":"MOOSE CREEK WELLS"},
                    {"Station ID":"26773","Name":"OMPAH-SEITZ"},
                    {"Station ID":"4333","Name":"OTTAWA CDA"},
                    {"Station ID":"30578","Name":"OTTAWA CDA RCS"},
                    {"Station ID":"49568","Name":"OTTAWA INTL A"},
                    {"Station ID":"49068","Name":"PEMBROKE CLIMATE"},
                    {"Station ID":"47527","Name":"PETAWAWA AWOS 2"},
                    {"Station ID":"4377","Name":"ST. ALBERT"},
                    {"Station ID":"6900","Name":"PETAWAWA HOFFMAN"},
                    {"Station ID":"43540","Name":"BALDWIN"},
                    {"Station ID":"50048","Name":"BARRIE LANDFILL"},
                    {"Station ID":"44183","Name":"BEATRICE CLIMATE"},
                    {"Station ID":"4432","Name":"COLDWATER WARMINSTER"},
                    {"Station ID":"10955","Name":"COLLINGWOOD"},
                    {"Station ID":"10911","Name":"LAGOON CITY"},
                    {"Station ID":"43046","Name":"MARKDALE"},
                    {"Station ID":"48368","Name":"MUSKOKA"},
                    {"Station ID":"6904","Name":"ORILLIA BRAIN"},
                    {"Station ID":"32128","Name":"PARRY SOUND CCG"},
                    {"Station ID":"4509","Name":"SHANTY BAY"},
                    {"Station ID":"42183","Name":"BARRIE-ORO"},
                    {"Station ID":"44303","Name":"SPRUCEDALE"},
                    {"Station ID":"44987","Name":"THORNBURY 3"},
                    {"Station ID":"4525","Name":"UDORA"},
                    {"Station ID":"54259","Name":"WIARTON A"},
                    {"Station ID":"53139","Name":"WIARTON A"},
                    {"Station ID":"7733","Name":"WESTERN ISLAND (AUT)"},
                    {"Station ID":"29516","Name":"BORDEN AWOS"},
                    {"Station ID":"27604","Name":"EGBERT CS"},
                    {"Station ID":"9004","Name":"COVE ISLAND (AUT)"},
                    {"Station ID":"7747","Name":"GODERICH"},
                    {"Station ID":"4575","Name":"KINCARDINE"},
                    {"Station ID":"48373","Name":"SARNIA"},
                    {"Station ID":"44323","Name":"SARNIA CLIMATE"},
                    {"Station ID":"45607","Name":"TOBERMORY RCS"},
                    {"Station ID":"4603","Name":"WROXETER"},
                    {"Station ID":"4607","Name":"AMHERSTBURG"},
                    {"Station ID":"52118","Name":"CHATHAM KENT"},
                    {"Station ID":"4619","Name":"CHATHAM WPCP"},
                    {"Station ID":"27528","Name":"DELHI CS"},
                    {"Station ID":"4635","Name":"FORT ERIE"},
                    {"Station ID":"44123","Name":"GRIMSBY MOUNTAIN"},
                    {"Station ID":"30266","Name":"HARROW CDA AUTO"},
                    {"Station ID":"4647","Name":"KINGSVILLE MOE"},
                    {"Station ID":"9026","Name":"LONG POINT (AUT)"},
                    {"Station ID":"4656","Name":"NEW GLASGOW"},
                    {"Station ID":"4671","Name":"PORT COLBORNE"},
                    {"Station ID":"7790","Name":"PORT WELLER (AUT)"},
                    {"Station ID":"32473","Name":"RIDGETOWN RCS"},
                    {"Station ID":"4680","Name":"RIDGEVILLE"},
                    {"Station ID":"50131","Name":"ST. CATHARINES / NIAGARA DISTRICT A"},
                    {"Station ID":"53000","Name":"ST CATHARINES A"},
                    {"Station ID":"4689","Name":"ST THOMAS WPCP"},
                    {"Station ID":"4699","Name":"TILLSONBURG WWTP"},
                    {"Station ID":"31367","Name":"VINELAND STATION RCS"},
                    {"Station ID":"44283","Name":"WELLAND-PELHAM"},
                    {"Station ID":"4715","Name":"WINDSOR RIVERSIDE"},
                    {"Station ID":"54738","Name":"WINDSOR A"},
                    {"Station ID":"9005","Name":"PORT COLBORNE (AUT)"},
                    {"Station ID":"9006","Name":"ERIEAU (AUT)"},
                    {"Station ID":"27533","Name":"POINT PELEE CS"},
                    {"Station ID":"53378","Name":"BRANTFORD AIRPORT"},
                    {"Station ID":"41983","Name":"ELORA RCS"},
                    {"Station ID":"4760","Name":"FERGUS SHAND DAM"},
                    {"Station ID":"4761","Name":"FERGUS MOE"},
                    {"Station ID":"45407","Name":"GUELPH TURFGRASS"},
                    {"Station ID":"48569","Name":"KITCHENER/WATERLOO"},
                    {"Station ID":"50093","Name":"LONDON A"},
                    {"Station ID":"10999","Name":"LONDON CS"},
                    {"Station ID":"7844","Name":"MOUNT FOREST (AUT)"},
                    {"Station ID":"4816","Name":"ROSEVILLE"},
                    {"Station ID":"27647","Name":"STRATHROY-MULLIFARRY"},
                    {"Station ID":"4835","Name":"WOODSTOCK"},
                    {"Station ID":"4859","Name":"BELLEVILLE"},
                    {"Station ID":"7868","Name":"BURLINGTON PIERS (AUT)"},
                    {"Station ID":"4898","Name":"CENTREVILLE"},
                    {"Station ID":"7870","Name":"COBOURG (AUT)"},
                    {"Station ID":"4905","Name":"COBOURG STP"},
                    {"Station ID":"4923","Name":"GEORGETOWN WWTP"},
                    {"Station ID":"49908","Name":"HAMILTON A"},
                    {"Station ID":"27529","Name":"HAMILTON RBG CS"},
                    {"Station ID":"32513","Name":"MOBILE UPPER AIR STATION-ONTARIO"},
                    {"Station ID":"45667","Name":"OAKVILLE TWN"},
                    {"Station ID":"48649","Name":"OSHAWA"},
                    {"Station ID":"4996","Name":"OSHAWA WPCP"},
                    {"Station ID":"7925","Name":"POINT PETRE (AUT)"},
                    {"Station ID":"51938","Name":"MONO CENTRE"},
                    {"Station ID":"31688","Name":"TORONTO CITY"},
                    {"Station ID":"48549","Name":"TORONTO CITY CENTRE"},
                    {"Station ID":"54239","Name":"TORONTO BUTTONVILLE A"},
                    {"Station ID":"53678","Name":"TORONTO BUTTONVILLE A"},
                    {"Station ID":"51459","Name":"TORONTO INTL A"},
                    {"Station ID":"30723","Name":"TRENTON AWOS"},
                    {"Station ID":"5126","Name":"TRENTON A"},
                    {"Station ID":"50637","Name":"UXBRIDGE WEST"},
                    {"Station ID":"26953","Name":"TORONTO NORTH YORK"},
                    {"Station ID":"29846","Name":"SOUTHERN ONTARIO EMERGENCY PORTABLE WEATHER STATION"},
                    {"Station ID":"5170","Name":"HALIBURTON 3"},
                    {"Station ID":"48952","Name":"PETERBOROUGH A"},
                    {"Station ID":"43763","Name":"PETERBOROUGH TRENT U"},
                    {"Station ID":"31010","Name":"SONYA SUNDANCE MEADOWS"},
                    {"Station ID":"44205","Name":"TAPLEY"},
                    {"Station ID":"26799","Name":"BANCROFT AUTO"}]


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


def download_ontario_data():
    ontario_weather = canadian_weather()
    for x in weather_station:
        
        ontario_weather_dataframe = ontario_weather.read_one_month_from_gc(x['Station ID'],datetime.now())
        if not ontario_weather_dataframe.empty:
            print(x)
            print(ontario_weather_dataframe)


        
       
download_ontario_data()


