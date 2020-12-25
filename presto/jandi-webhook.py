import logging
import requests

my_headers = {"Content-Type": "application/json", "Accept": "application/vnd.tosslab.jandi-v2+json"}
my_data = '{"body": "hello"}'

resp = requests.post(
  "https://wh.jandi.com/connect-api/webhook/12835000/99b44417e7de2cd2df08437f1788d521",
  data=my_data,
  headers=my_headers,
  timeout=5.0
)
if resp.status_code != 200:
  logging.error(
    "webhook send ERROR. status_code => {status}".format(
    status=resp.status_code
  )
)
print( resp )
