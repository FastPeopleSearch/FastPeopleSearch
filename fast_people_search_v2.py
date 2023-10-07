'''
Author: Ashish Venkat Barad
Email: ashishvenkatbarad@gmail.com
Purpose: To scrape fastpeoplesearch.com without getting blocked
'''

import json
#$ from credentials import PROXY_ADDRESS, PROXY_USER, PROXY_PASS
import random
from io import StringIO
from multiprocessing.pool import ThreadPool
import requests
import pandas as pd
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

thread_count = 6
fast_people_names_and_locations = tuple()
fast_pass_data_result = tuple()
indeed_results = tuple()



def getProxies():
    resp = requests.get('https://free-proxy-list.net/')
    soup = BeautifulSoup(resp.content,'html.parser')
    df = pd.read_html(StringIO(str(soup.find('table'))))[0]
    ips = json.loads(df.to_json(orient='records'))
    ip_list=[]
    for i in ips:
        if i['Https'] == 'no' and i['Anonymity'] == 'elite proxy':
            ip_address = i['IP Address']
            port = i['Port']
            proxy = {"http": f"http://{ip_address}:{port}",}
            r = requests.get("https://lumtest.com/echo.json", proxies=proxy)
            if r.status_code == 200:
                ip_list.append(proxy)
    return ip_list



def split_tasks(total_rows, num_threads):
    start_index = 0
    increment = total_rows // num_threads
    remainder = total_rows % num_threads
    args = ()
    while start_index < (total_rows - remainder):
        end_index = start_index + increment
        args += ((start_index, end_index,),)
        start_index += increment
    args += ((start_index, total_rows,),)
    return args


def get_proxy():
    random_proxy = random.choice(getProxies())
    proxy = {
        "server": random_proxy['http'].replace("http://", "")
    }
    return proxy
    # return {
    #     "server": PROXY_ADDRESS,
    #     "username": PROXY_USER,
    #     "password": PROXY_PASS
    # }


def handle_route(route, request):
    route.continue_() if ((route.request.resource_type == 'document') or (
        'email-decode.' in route.request.url)) else route.abort()


def get_attr(elem, attr="value", default_val=""):
    return elem.get(attr, default_val).strip() if elem else default_val


base_url = 'https://www.fastpeoplesearch.com'
name_url = 'https://www.fastpeoplesearch.com/name/'
url1 = 'https://lumtest.com/echo.json'


def fast_people_scrape(indexes):
    global fast_people_names_and_locations
    global fast_pass_data_result
    start_index, end_index = indexes
    row = start_index
    total = end_index
    while row < total:
        with sync_playwright() as p:
            browser = p.firefox.launch()
            new_proxy = get_proxy()
            context = browser.new_context(proxy=new_proxy)
            page = context.new_page()
            candidate = fast_people_names_and_locations[row]
            name = candidate[0]
            place = candidate[1]
            try:
                page.route('**/*', handle_route)
                # final_url = 'https://www.fastpeoplesearch.com/fernando-salvatierra_id_G-6006063028242892593'
                print(row)
                page.goto(url1)
                print(json.loads(BeautifulSoup(page.content(), 'html.parser').get_text())['ip'])

                page.goto(
                    f"{name_url}{name.lower().replace(' ', '-')}_{place.lower().replace(',', '').replace(' ', '-')}")
                page_content = page.content()
                soup = BeautifulSoup(page_content, 'html.parser')

                if page_content.find("Your access has been blocked due to abnormal activity from your IP") != -1:
                    print("Blocked: reseting proxy")
                    continue

                # assert ('FastPeopleSearch.com'.lower() == get_attr(soup.find('meta', {'name': 'application-name'}),
                #                                                   'content', '').lower().strip())
                path = soup.find('a', {'class': 'link-to-details'})
                print(path)
                if not path:
                    fast_pass_data_result += ((name, place, "NOT FOUND", "NOT FOUND",),)
                    print((name, place, "NOT FOUND", "NOT FOUND",))
                    row += 1
                    continue
                path = get_attr(path, 'href')
                final_url = f'{base_url}{path}'
                page.goto(final_url)
                soup = BeautifulSoup(page.content(), 'html.parser')
                phone = soup.select_one('div.detail-box-phone strong a')
                phone = phone.get_text(strip=True) if phone else 'Not Found'
                email = soup.select_one('div#email_section h3')
                email = email.get_text(strip=True) if email else 'Not Found'
                fast_pass_data_result += ((name, place, phone, email,),)
                print((name, place, phone, email,))
                percentage_complete = (row - start_index) / (total - start_index)
                print(f'{row} of {total}: {percentage_complete:.2f}%')
                row += 1
                page.close()
                browser.contexts.clear()
            except Exception as e:
                print(f'error in proxy {e}:')
                print("\tName and place: ", name, place)

                # context = browser.new_context(proxy=get_proxy())
                # page = context.new_page()
                print("error")
                fast_pass_data_result += ((name, place, "ERROR", "ERROR",),)
                row += 1



def fast_people_collection(data, threads=None, test=True):
    global fast_pass_data_result
    global fast_people_names_and_locations
    fast_people_names_and_locations = data
    fast_pass_data_result = tuple()
    if threads is not None:
        indexes = split_tasks(len(data), threads)
        print(indexes)
        print(len(indexes))
        print(len(indexes[0]))
        with ThreadPool(threads) as p:
            p.map(fast_people_scrape, indexes)
            p.close()
            p.join()

    else:
        fast_people_scrape((0, len(data),))
    if test:
        import json
        with open('fastpassdataresult.json', 'w') as f:
            f.write(json.dumps(fast_pass_data_result))
    return fast_pass_data_result
