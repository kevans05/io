import ieso_scraper, database

import pandas as pd
import numpy as np

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
        print('date does not exist')
        #ieso_sql.to_sql_independent_electrical_system_operator_statistics(ieso_data.return_start_date(),
        #                                                                  ieso_data.return_created_at())

        dfObj = pd.concat([ieso_data.return_actual_data(),ieso_sql.return_sql_table('actual')],axis=0, join='outer',
                          ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, copy=True)

            #.drop_duplicates(subset ="datetime",
            #         keep = False, inplace = True)

            #pd.merge(ieso_data.return_actual_data().set_index('datetime'),ieso_sql.return_sql_table('actual').set_index('datetime'))
        print(ieso_data.return_actual_data())
        print(ieso_sql.return_sql_table('actual'))
        print(dfObj)
        #ieso_sql.to_sql(dfObj, 'actual')

    else:
        print('date exist')
else:
    ieso_sql.to_sql_independent_electrical_system_operator_statistics(ieso_data.return_start_date(),
                                                                      ieso_data.return_created_at())
    ieso_sql.to_sql(ieso_data.return_actual_data(), 'actual')
    ieso_sql.to_sql(ieso_data.return_five_minute_data(), 'five_minute')
    ieso_sql.to_sql(ieso_data.return_projected_data(), 'projected_' + str(ieso_data.return_created_at().timestamp()))

