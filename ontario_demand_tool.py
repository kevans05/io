import datetime as dt
import pandas as pd
import xml.etree.ElementTree as xml

from sqlalchemy import create_engine
from time import sleep

import tempfile
import requests
import datetime

class RetrieveIndependentElectricalSystemOperatorDemandData:
    #########################################################
    #  Method name: __init__
    #  Parameters: self
    #  Return: Nan
    #  Functionality: initial values for the class
    #########################################################
    def __init__(self):
        # connecting to a SQLite database
        self.url = 'http://www.ieso.ca/-/media/files/ieso/uploaded/chart/ontario_demand_multiday.xml?la=en'
        self.start_date = dt.datetime(1970,1,1,0,0,0)
        self.created_at = dt.datetime(1970,1,1,0,0,0)
        self.five_minute_data = pd.DataFrame(columns=['datetime', 'value'])
        self.actual_data = pd.DataFrame(columns=['datetime', 'value'])
        self.projected_data = pd.DataFrame(columns=['datetime', 'value'])
        self.db = 'sqlite:///database.db'

        self.five_minute_data.set_index(pd.DatetimeIndex(self.five_minute_data['datetime']))

    #########################################################
    #  Method name: return_target_url
    #  Parameters: self
    #  Return: string containing current url
    #  Functionality: returning a value
    #########################################################
    def return_target_url(self):
        return self.url

    #########################################################
    #  Method name: update_target_url
    #  Parameters: string value containing an active url
    #  Return: nan
    #  Functionality: takes a new url and check to see if it
    #  is valid before updating the class variable url
    #########################################################
    def update_target_url(self, new_target_url):
        self.url = new_target_url

    #########################################################
    #  Method name: return_start_date
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def return_start_date(self):
        return self.start_data

    #########################################################
    #  Method name: return_created_at
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def return_created_at(self):
        return self.created_at

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
    #  Method name: parse_five_minuter_data
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def parse_five_minuter_data(self, dataset):
        datetime_object = self.start_date
        dfObj = pd.DataFrame(columns=['datetime', 'value'])
        for x in dataset:
            for y in x:
                dfObj = dfObj.append({'datetime': datetime_object, 'value': y.text}, ignore_index=True)
                datetime_object = datetime_object + datetime.timedelta(minutes=5)
        dfObj.set_index(pd.DatetimeIndex(dfObj['datetime']))
        self.five_minute_data = self.five_minute_data.merge(dfObj, how='outer').dropna()

    #########################################################
    #  Method name: parse_actual_data
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def parse_actual_data(self, dataset):
        datetime_object = self.start_date
        dfObj = pd.DataFrame(columns=['datetime', 'value'])
        for x in dataset:
            for y in x:
                dfObj = dfObj.append({'datetime': datetime_object, 'value': y.text}, ignore_index=True)
                datetime_object = datetime_object + datetime.timedelta(minutes=60)
        dfObj.set_index(pd.DatetimeIndex(dfObj['datetime']))
        self.actual_data = self.actual_data.merge(dfObj, how='outer').dropna()

    #########################################################
    #  Method name: parse_projected_data
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def parse_projected_data(self, dataset):
        datetime_object = self.start_date
        dfObj = pd.DataFrame(columns=['datetime', 'value'])
        for x in dataset:
            for y in x:
                dfObj = dfObj.append({'datetime': datetime_object, 'value': y.text}, ignore_index=True)
                datetime_object = datetime_object + datetime.timedelta(minutes=60)
        dfObj.set_index(pd.DatetimeIndex(dfObj['datetime']))
        self.projected_data = self.projected_data.merge(dfObj, how='outer').dropna()

    #########################################################
    #  Method name: five_minute_to_sql
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def five_minute_to_sql(self):
        engine = create_engine(self.db, echo=False)
        try:
            dfObj = pd.read_sql('five_minute_data',con=engine,index_col='datetime')
            dfObj = dfObj.merge(self.five_minute_data, how='outer').dropna()
            dfObj.to_sql('five_minute_data', con=engine, if_exists='append')
        except:
            self.five_minute_data.to_sql('five_minute_data', con=engine, if_exists='replace')

    #########################################################
    #  Method name: actual_to_sql
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def actual_to_sql(self):
        engine = create_engine(self.db, echo=False)
        try:
            dfObj = pd.read_sql('actual', con=engine, index_col='datetime')
            dfObj = dfObj.merge(self.actual_data, how='outer').dropna()
            dfObj.to_sql('actual', con=engine, if_exists='append')
        except:
            self.actual_data.to_sql('actual', con=engine, if_exists='replace')



    #########################################################
    #  Method name: projected_to_sql
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def projected_to_sql(self):
        engine = create_engine(self.db, echo=False)
        self.projected_data.to_sql('projected_' + str(self.created_at.timestamp()), con=engine, if_exists='append')

    #########################################################
    #  Method name: download_data_set
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def download_data_set(self):
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
                engine = create_engine(self.db, echo=False)
                dfObj = pd.DataFrame(columns=['created_at', 'start_date', 'downloaded_at'])
                dfObj = dfObj.append({'created_at': self.created_at, 'start_date': self.start_date,
                                      'downloaded_at': datetime.datetime.now()}, ignore_index=True)
                dfObj.to_sql('independent_electrical_system_operator_statistics', con=engine)
                for dataset in root.iter('DataSet'):
                    if dataset.attrib.get("Series") in '5_Minute':
                        self.parse_five_minuter_data(dataset)
                    elif dataset.attrib.get("Series") in 'Actual':
                        self.parse_actual_data(dataset)
                    elif dataset.attrib.get("Series") in 'Projected':
                        self.parse_projected_data(dataset)
                print('New Data Added')
            except:
                print('Already Exists')
        finally:
            temp.close()
        return None



Demand_data = RetrieveIndependentElectricalSystemOperatorDemandData()
Demand_data.download_data_set()
Demand_data.five_minute_to_sql()
Demand_data.actual_to_sql()
Demand_data.projected_to_sql()
f= open("lastRun.txt","w+")
f.write("%s\r\n" % str(dt.datetime.now()))
f.close()