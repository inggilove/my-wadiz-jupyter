from pyhive import presto
URL_WADI_TAG = '10.254.254.183'
presto_connection = presto.connect(
    host=URL_WADI_TAG, 
    port=8080,
    schema='waditag', 
    catalog='hive')

import pandas as pd
sql_get_keywords = """
select replace( tagurl, 'Search_', '' ) as keyword, Count(*) AS Rank
  from waditag.orc_tagevent
    where url = '/web/wstartup/main' and action = '2020' 
    and tagurl not like 'Search_Filter%' and tagurl like 'Search_%'
    and tagurl != 'Search_'
    -- and date(createdat_date) >= current_date - interval '1' day and date(createdat_date) < current_date
    and date(createdat_date) = current_date
    group by 1
    order by Rank desc 
    limit 100
"""
df_keywords = pd.read_sql_query( sql_get_keywords , presto_connection)
print( df_keywords )
