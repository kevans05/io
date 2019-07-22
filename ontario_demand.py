import requests as rq
import tempfile as tf
import xml.etree.ElementTree as et
import datetime as dt
import dataset as ds
import pandas as pd
class ontario_demand:
    #########################################################
    #  Method name: __init__
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def __init__(self, url = 'http://www.ieso.ca/-/media/Files/IESO/uploaded/Chart/ontario_demand_multiday.xml?la=en'):
        # connecting to a SQLite database
        self.url = url
        self.start_date = dt.datetime(1970,1,1,0,0,0)
        self.created_at = dt.datetime(1970,1,1,0,0,0)
        self.five_minute_data = []
        self.actual_data = []
        self.projected_data = []

    #########################################################
    #  Method name: __init__
    #  Parameters:
    #  Return:
    #  Functionality:
    #########################################################
    def check_ieso(self):
        print("Creating one named temporary file...")

        temp = tf.NamedTemporaryFile()
        try:
            print("Created file is:", temp)
            print("Name of the file is:", temp.name)
            response = rq.get(self.url)
            with open(temp.name, 'wb') as file:
                file.write(response.content)
            tree = et.parse('feed.xml')
            root = tree.getroot()
            if dt.datetime.strptime(root.find('created_at').text, "%Y-%m-%dT%H:%M:%S") > self.created_at:
                self.start_data = dt.datetime.strptime(root.find('StartDate').text, "%Y-%m-%dT%H:%M:%S")
                self.created_at = dt.datetime.strptime(root.find('CreatedAt').text, "%Y-%m-%dT%H:%M:%S")
                for dataset in root.iter('DataSet'):
                    datetime_object = self.start_data
                    for values in dataset:
                        for value in values:
                            if dataset.attrib.get("Series") in '5_Minute':
                                self.five_minute_data.append({'datetime': datetime_object,'value':value.text})
                                datetime_object = datetime_object + dt.timedelta(minutes=5)
                            elif dataset.attrib.get("Series") in 'Actual':
                                self.actual_data.append({'datetime': datetime_object, 'value': value.text})
                                datetime_object = datetime_object + dt.timedelta(minutes=60)
                            elif dataset.attrib.get("Series") in 'Projected':
                                self.projected_data.append({'datetime': datetime_object, 'value': value.text})
                                datetime_object = datetime_object + dt.timedelta(minutes=60)
        finally:
            print("Closing the temp file")
            temp.close()
        return 0





def main():
    OD = ontario_demand
    OD.check_ieso()
    # connecting to a SQLite database


    while(True):
        db = dt.connect('sqlite:///mydatabase.db')

        # get a reference to the table 'user'
        table_5_minute = db['ontario_demand_5_minute']
        table_60_minute = db['ontario_demand_60_minute']
        table_demand_predictions = db['ontario_demand_predictions' & OD.created_at]

        df_5_minute = pd.DataFrame(columns=['datetime', 'value'])
        df_5_minute.merge(pd.DataFrame(table_5_minute))

        df_60_minute = pd.DataFrame(columns=['datetime', 'value'])
        df_60_minute.merge(pd.DataFrame(table_5_minute))

        table_demand_predictions

main()