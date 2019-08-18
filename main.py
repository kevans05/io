import ieso_scraper, database


import pandas as pd


#sets up a ieso scraper
ieso_data = ieso_scraper.scrape_ieso()
#sets up the sql builder
ieso_sql = database.ieso_sql()

ieso_data.update_data()

independent_electrical_system_operator_statistics = ieso_sql.return_sql_table('independent_electrical_system_operator_statistics')
if not independent_electrical_system_operator_statistics.empty:
    independent_electrical_system_operator_statistics.set_index(['created_at'])
    if ieso_data.return_created_at() in independent_electrical_system_operator_statistics.index:
        ieso_sql.to_sql_independent_electrical_system_operator_statistics(ieso_data.return_start_date(),
                                                                          ieso_data.return_created_at())



        ieso_sql.to_sql(pd.merge(ieso_data.return_actual_data().set_index('datetime'),
                 ieso_sql.return_sql_table('actual').set_index('datetime'), on='datetime', how='outer'), 'actual')
        ieso_sql.to_sql(ieso_data.return_five_minute_data(), 'five_minute')
        ieso_sql.to_sql(ieso_data.return_projected_data(), 'projected'&ieso_data.return_created_at().timestamp())

    else:
        print("fuck"*300)
else:
    ieso_sql.to_sql_independent_electrical_system_operator_statistics(ieso_data.return_start_date(),
                                                                      ieso_data.return_created_at())
    ieso_sql.to_sql(ieso_data.return_actual_data(), 'actual')
    ieso_sql.to_sql(ieso_data.return_five_minute_data(), 'five_minute')
    ieso_sql.to_sql(ieso_data.return_projected_data(), 'projected_' + str(ieso_data.return_created_at().timestamp()))


