# 1. Startup Search Keyword

from pyhive import presto
URL_WADI_TAG = '10.254.254.183'
presto_connection = presto.connect(
    host=URL_WADI_TAG, 
    port=8080,
    schema='waditag', 
    catalog='hive')

print( "Start..." )

import pandas as pd
sql_get_keywords = """
select createdat_date, trim(replace( tagurl, 'Search_', '' )) as keyword, Count(*) AS Rank
  from waditag.orc_tagevent
    where url = '/web/wstartup/main' and action = '2020' 
    and tagurl not like 'Search_Filter%' and tagurl like 'Search_%'
    and tagurl != 'Search_'
    and date(createdat_date) >= current_date - interval '1' day and date(createdat_date) < current_date
    -- and date(createdat_date) = current_date
    group by 1,2
    order by Rank desc 
    limit 10
"""
df_keywords = pd.read_sql_query( sql_get_keywords , presto_connection)
print( "..." )
print( df_keywords.head() )

# 2. Format data
def list2str_newline( df_list ):
  str_result = ''
  cnt = 0
  for i in df_list:
    if cnt != 0:
      str_result += '\\n'
    str_result += "{0}".format( i )
    cnt += 1
  #print(str_result)
  return str_result

search_date = df_keywords['createdat_date'][0]
keyword_list = list2str_newline( df_keywords.loc[:,['keyword','Rank']].values.tolist() ) 

hook_title = "[ {0} ] 스타트업찾기 [[검색 키워드 랭킹]](http://dcbi.wadizcorp.com/public/dashboards/Cr0D3CmXYePTZjBs5XvSsiix1yK4omWGiZyM84JZ?org_slug=default)".format(search_date )
hook_body = '[{{"title": "검색어 Top 10 (검색어:조회수)", "description" : "{0}" }}]'.format( keyword_list )
#hook_url = "https://wh.jandi.com/connect-api/webhook/12835000/99b44417e7de2cd2df08437f1788d521"
#hook_url = "https://wh.jandi.com/connect-api/webhook/12835000/252921d3701f3b0d175ec259fb87d68b"
hook_url = "https://wh.jandi.com/connect-api/webhook/12835000/58bde7be5455696c25112eb8025d4f62"

print( '{{"body": "{0}","connectInfo": {1} }}'.format(hook_title, hook_body ) )

# 3. POST Send to Jandi

import logging
import requests

my_headers = {
    "Content-Type": "application/json", 
    "Accept": "application/vnd.tosslab.jandi-v2+json"
}
my_data_raw = '{{"body": "{0}","connectInfo": {1} }}'.format(hook_title, hook_body )
my_data = my_data_raw.encode( 'utf-8' )

resp = requests.post(
  hook_url,
  headers=my_headers,
  data=my_data,
  timeout=5.0
)
if resp.status_code != 200:
  logging.error(
    "webhook send ERROR. status_code => {status}".format(
    status=resp.status_code
  )
)
print( "Send OK - {0} : {1} ".format( hook_url, my_data ) )
