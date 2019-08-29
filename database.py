from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd

class ieso_sql:
    ########################################################
    #  Method name: __init__
    #  Parameters: self
    #  Return: Nan
    #  Functionality: initial values for the class
    #########################################################
    def __init__(self):
        self.db_local = 'sqlite:///database.db'
        self.db_remote = 'postgresql://ieso_scraper:x@localhost:5433/ieso'

    ########################################################
    #  Method name: to_sql
    #  Parameters: self
    #  Return: Nan
    #  Functionality: initial values for the class
    #########################################################
    def to_sql(self, dfObj, table):

        try:
            engine = create_engine(self.db_remote, echo=False)
            dfObj.to_sql(table, con=engine, if_exists='replace')
        except:
            engine = create_engine(self.db_local, echo=False)
            dfObj.to_sql(table, con=engine, if_exists='replace')
    ########################################################
    #  Method name: to_sql
    #  Parameters: self
    #  Return: Nan
    #  Functionality: initial values for the class
    #########################################################
 
    def to_sql_independent_electrical_system_operator_statistics(self, start_date, created_at):
        dfObjMaster = pd.DataFrame(columns=['created_at', 'start_date', 'downloaded_at'])
        dfObjMaster = dfObjMaster.append({'created_at': created_at, 'start_date': start_date,
                                          'downloaded_at': datetime.now()}, ignore_index=True)
        try:
            engine = create_engine(self.db_remote, echo=False)
            dfObjMaster.to_sql('independent_electrical_system_operator_statistics', con=engine, if_exists='append')
        except:
            engine = create_engine(self.db_local, echo=False)
            dfObjMaster.to_sql('independent_electrical_system_operator_statistics', con=engine, if_exists='append')




    ########################################################
    #  Method name: return_sql
    #  Parameters: self
    #  Return: Nan
    #  Functionality: initial values for the class
    #########################################################
    def return_sql_table(self, table):
        try:
            try:
                engine = create_engine(self.db_remote, echo=False)
                sql_actual_data_frame = pd.read_sql(table, con=engine)
            except:
                engine = create_engine(self.db_local, echo=False)
                sql_actual_data_frame = pd.read_sql(table, con=engine)
            return sql_actual_data_frame
        except:
            return pd.DataFrame()
class weather_sql:
    ########################################################
    #  Method name: __init__
    #  Parameters: self
    #  Return: Nan
    #  Functionality: initial values for the class
    #########################################################
    def __init__(self):
        self.db_local = 'sqlite:///database.db'
        self.db_remote = 'postgresql://ieso_scraper:x@localhost:5433/ieso'

    ########################################################
    #  Method name: to_sql
    #  Parameters: self
    #  Return: Nan
    #  Functionality: initial values for the class
    #########################################################
    def to_sql(self, dfObj, table):

        try:
            engine = create_engine(self.db_remote, echo=False)
            dfObj.to_sql(table, con=engine, if_exists='replace')
        except:
            engine = create_engine(self.db_local, echo=False)
            dfObj.to_sql(table, con=engine, if_exists='replace')
    
    
    def to_sql_last_download(self, datetime_value):
        dfObjMaster = pd.DataFrame(columns=['last_download'])

        dfObjMaster = dfObjMaster.append({'last_download': datetime.now()}, ignore_index=True)
        try:
            engine = create_engine(self.db_remote, echo=False)
            dfObjMaster.to_sql('last_download', con=engine, if_exists='append')
        except:
            engine = create_engine(self.db_local, echo=False)
            dfObjMaster.to_sql('last_download', con=engine, if_exists='append')


    ########################################################
    #  Method name: return_sql
    #  Parameters: self
    #  Return: Nan
    #  Functionality: initial values for the class
    #########################################################
    def return_sql_table(self, table):
        try:
            try:
                engine = create_engine(self.db_remote, echo=False)
                sql_actual_data_frame = pd.read_sql(table, con=engine)
            except:
                engine = create_engine(self.db_local, echo=False)
                sql_actual_data_frame = pd.read_sql(table, con=engine)
            return sql_actual_data_frame
        except:
            return pd.DataFrame()
