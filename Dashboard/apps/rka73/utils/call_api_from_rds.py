import requests
import pandas as pd

from apps.rka73.data import data as d1

shouldCallAPI = True

def call_api(file_name):
  if shouldCallAPI:
    air_type_name = d1.get_tname_from_filename(file_name)
    api_requests = requests.get(f"http://fire-forrest-maps.herokuapp.com/v1/fire/get_all_air_quality_data/{air_type_name}")
    res_data = api_requests.json()['data']
    df = pd.DataFrame(res_data)
  else:
    df = pd.read_csv(file_name)
  return df