import datetime as dt
import pandas as pd
import xml.etree.ElementTree as xml

import tempfile
import requests

class scrape_ieso:
    ########################################################
    #  Method name: __init__
    #  Parameters: self
    #  Return: Nan
    #  Functionality: initial values for the class
    #########################################################
    def __init__(self):
        self.url = 'http://www.ieso.ca/-/media/files/ieso/uploaded/chart/ontario_demand_multiday.xml?la=en'
        self.start_date = dt.datetime(1970, 1, 1, 0, 0, 0)
        self.created_at = dt.datetime(1970, 1, 1, 0, 0, 0)
        self.five_minute_data = pd.DataFrame(columns=['datetime', 'value'])
        self.actual_data = pd.DataFrame(columns=['datetime', 'value'])
        self.projected_data = pd.DataFrame(columns=['datetime', 'value'])

    def __parse_data(self, dataset, duration):
        datetime_object = self.start_date
        df_obj = pd.DataFrame(columns=['datetime', 'value'])
        for x in dataset:
            for y in x:
                df_obj = df_obj.append({'datetime': datetime_object, 'value': y.text}, ignore_index=True)
                datetime_object = datetime_object + dt.timedelta(minutes=duration)
        df_obj.set_index(pd.DatetimeIndex(df_obj['datetime']))
        return df_obj.dropna()

    #########################################################
    #  Method name: return_five_minute_data
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def update_data(self):
        temp = tempfile.NamedTemporaryFile()
        try:
            response = requests.get(self.url)
            with open(temp.name, 'wb') as file:
                file.write(response.content)
            tree = xml.parse(temp.name)
            root = tree.getroot()
            self.start_date = dt.datetime.strptime(root.find('StartDate').text, "%Y-%m-%dT%H:%M:%S")
            self.created_at = dt.datetime.strptime(root.find('CreatedAt').text, "%Y-%m-%dT%H:%M:%S")
            try:
                for dataset in root.iter('DataSet'):
                    if dataset.attrib.get("Series") in '5_Minute':
                        self.five_minute_data = self.__parse_data(dataset,5)
                    elif dataset.attrib.get("Series") in 'Actual':
                        self.actual_data = self.__parse_data(dataset,60)
                    elif dataset.attrib.get("Series") in 'Projected':
                        self.projected_data = self.__parse_data(dataset,60)
            except:
                print('Failed')
        finally:
            temp.close()
        return None

    #########################################################
    #  Method name: return_five_minute_data
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def return_five_minute_data(self):
        return self.five_minute_data

    #########################################################
    #  Method name: return_actual_data
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def return_actual_data(self):
        return self.actual_data

    #########################################################
    #  Method name: return_actual_data
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def return_projected_data(self):
        return self.projected_data

    #########################################################
    #  Method name: return_start_date
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def return_start_date(self):
        return self.start_date

    #########################################################
    #  Method name:
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def return_created_at(self):
        return self.created_at

