from fast_people_search_v2 import getProxies
from fast_people_search_v2 import fast_people_collection
import pandas as pd

def test_get_free_proxies():
    proxy_list = getProxies()
    print(proxy_list)

def test_fast_pass_bulk_search():
    df = pd.read_csv("indeed_test_data.csv")
    indeed_names_and_locations = df[['DefaultName', 'location']].values
    result = fast_people_collection(indeed_names_and_locations)
    print(result)