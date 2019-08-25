import ieso_scraper, database

import pandas as pd
import numpy as np

def concat_dataframes(sql_dataframe, live_dataframe):
    sql_max = max(sql_dataframe['datetime'])
    mask = (live_dataframe['datetime'] > sql_max)
    live_dataframe = live_dataframe.loc[mask]
    df_concat = pd.concat([sql_dataframe.set_index(['datetime']), live_dataframe.set_index(['datetime'])], sort=True)
    return df_concat


#sets up a ieso scraper
ieso_data = ieso_scraper.scrape_ieso()
#sets up the sql builder
ieso_sql = database.ieso_sql()

ieso_data.update_data()

independent_electrical_system_operator_statistics = \
    ieso_sql.return_sql_table('independent_electrical_system_operator_statistics')

if not independent_electrical_system_operator_statistics.empty:
    independent_electrical_system_operator_statistics.created_at = \
        pd.to_datetime(independent_electrical_system_operator_statistics.created_at)
    if not(independent_electrical_system_operator_statistics.created_at ==
           pd.Timestamp(ieso_data.return_created_at())).any():
        ieso_sql.to_sql_independent_electrical_system_operator_statistics(ieso_data.return_start_date(),
                                                                          ieso_data.return_created_at())

        df_concat = concat_dataframes(ieso_sql.return_sql_table('actual'), ieso_data.return_actual_data())
        ieso_sql.to_sql(df_concat,'actual')

        df_concat = concat_dataframes(ieso_sql.return_sql_table('five_minute'), ieso_data.return_five_minute_data())
        ieso_sql.to_sql(df_concat,'five_minute')
        
        ieso_sql.to_sql(ieso_data.return_projected_data().set_index(['datetime']), 'projected_' + str(ieso_data.return_created_at().timestamp()))

        print('database exists,data does not exist')
    else:
        print('database exists,data exist')
else:
    ieso_sql.to_sql_independent_electrical_system_operator_statistics(ieso_data.return_start_date(),
                                                                      ieso_data.return_created_at())
    ieso_sql.to_sql(ieso_data.return_actual_data().set_index(['datetime']), 'actual')
    ieso_sql.to_sql(ieso_data.return_five_minute_data().set_index(['datetime']), 'five_minute')
    ieso_sql.to_sql(ieso_data.return_projected_data().set_index(['datetime']), 'projected_' + str(ieso_data.return_created_at().timestamp()))
    print('new database')
