import asyncio
import re
import time
from selenium.webdriver.chrome.options import Options

import aiohttp


import apiclient
import httplib2
import psycopg2
import requests
from aiohttp import ClientTimeout
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By

from config import host, user, password, db_name


FILE_JSON = 'bitriks-413311-b6a6348d8b48.json'
TABLE = "bitriks"

def writing_to_the_excel():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        FILE_JSON,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive'])
    httpAuth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)

    # подключение к БД
    connection = None
    cursor = None
    list = []
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM "the_company"')
        list = cursor.fetchall()

    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("[INFO] PostgreSQL connection closed")

    j = 0
    for i in list:
        values = service.spreadsheets().values().batchUpdate(
            spreadsheetId='1MLa5aAUoMVk1J3kqQf7tRNHpjewjxBvv4fGriAv_lIc',
            body={
                'valueInputOption': 'USER_ENTERED',
                'data': [
                    {
                        'range': f'A{i[0] + 1}',
                        'majorDimension': 'COLUMNS',
                        'values': [[i[0]], [i[1]], [i[2]], [i[3]], [i[4]], [i[5]], [i[6]], [i[7]], [i[8]], [i[9]]]

                    }
                ]
            }
        ).execute()
        print(j)
        if j % 20 == 0:
            time.sleep(50)
        j += 1




def writing_to_the_database(list):
    connection = None

    cursor = None
    try:

        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,

            connect_timeout=10,
            # https://www.postgresql.org/docs/9.3/libpq-connect.html
            keepalives=1,
            keepalives_idle=5,
            keepalives_interval=2,
            keepalives_count=2
        )

        connection.autocommit = True

        cursor = connection.cursor()

        cursor.execute("SELECT version();")
        print(f"Server version: {cursor.fetchone()}")


        cursor.execute('SELECT * FROM "the_company"')

        list_bd = cursor.fetchall()

        #writing_to_the_excel(list_bd)
        flag = True

        flag_overwrites = False

        #проверка на повторную запись
        for i in list:
            for j in list_bd:
                if i[0] == j[1] and i[1] == j[2] and i[4] == j[5] and \
                        i[6] == j[7] and i[8] == j[9]:

                    flag = False
                else:
                    # проверка на перезапись существующей информации
                    if i[0] == j[1]:
                        id = j[0]
                        flag_overwrites = True
                        flag = False


        # если повторной записи нет, записываем в бд
        if flag:
            cursor.execute(
                'INSERT INTO the_company ("Name", "Description", "Rating", "Number of reviews", "Link", "API", "Link API",'
                '"Affiliate Program", "Link Affiliate Program") VALUES '
                '(%s,%s,%s,%s,%s,%s,%s,%s,%s)', (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8]))

        if flag_overwrites:
            cursor.execute(
                'UPDATE the_company SET "Name" = %s, "Description" = %s, "Rating" = %s, "Number of reviews" = %s, '
                '"Link" = %s, "API" = %s, "Link API" = %s,'
                '"Affiliate Program" = %s, "Link Affiliate Program" = %s WHERE "ID" = %s',
                (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], id))

        cursor.execute('SELECT * FROM "the_company"')




    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
    finally:
        if connection:

            cursor.close()
            connection.close()
            print("[INFO] PostgreSQL connection closed")

#####################################################
URL = 'https://www.producthunt.com/categories'
URL_content = 'https://www.producthunt.com'
HEADERS = {'user-agent' : 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/93.0.4577.82 YaBrowser/21.9.0.1044 Yowser/2.5 Safari/537.36',
           'accept': '*/*'}



content_url = ''
button_protection_url = ''


def get_html(url, params = None) :
    print("get_html")
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}, params=params)
    return r



html_api = '-'
html_affiliate = '-'
bool_api = 'No'
bool_affiliate = 'No'
FIELDS = [
    {"h1": "color-darker-grey md:fontSize-32 sm:fontSize-32 fontSize-18 fontWeight-700", "div" : ["color-darker-grey fontSize-16 fontWeight-600 mb-3", "color-lighter-grey fontSize-16 fontWeight-400 mb-6"]
     , "a" : "color-lighter-grey fontSize-14 fontWeight-400 styles_count___6_8F", "button" : '//*[@id="__next"]/div[3]/main/div[3]/button'},
    {"h1": "color-dark-grey md:fontSize-32 sm:fontSize-32 fontSize-18 fontWeight-700", "div" : ["color-dark-grey fontSize-16 fontWeight-600 mb-3", "color-light-grey fontSize-16 fontWeight-400 mb-6"]
        , "a" : "color-light-grey fontSize-14 fontWeight-400 styles_count___6_8F", "button" : '//*[@id="__next"]/div[2]/main/div[3]/button'}

]
#парсинг информации о компании
async def get_card(html, session):
    list = []

    print("1")
    headers = {
        "User-Agent": "Mozilla / 5.0(Windows NT 10.0; Win64; x64) AppleWebKit "
                      "/ 537.36(KHTML, like Gecko) Chrome / 116.0 .5845 .686 YaBrowser / 23.9 .0 .0 Safari / 537.36"
    }
    print("2")
    #print(html + "111111111")
    try:
        async with session.get(url=html, headers={"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}) as response:
            #print(html)
            print("-------------------------------------------------")
            print(response)
            html1 = await response.text()
            soup = BeautifulSoup(html1, 'html.parser')

            for i in range(len(FIELDS)):
                INDEX = i
                items = soup.find('h1', class_= FIELDS[INDEX]['h1'])
                if items:
                    break

            #парсинг названии компании
            try:
                name = items.get_text()
                print(name)
            except:
                name = '-'
                #return

            #парсинг описания
            try:
                des = soup.find_all('div', class_='flex flex-column')

                descrip1 = des[1].find('div', class_ = FIELDS[INDEX]['div'][0])
                descrip2 = des[1].find('div', class_ = FIELDS[INDEX]['div'][1])

                descrip = descrip1.get_text() + descrip2.get_text()
                print(descrip)
            except:

                descrip = '-'
                #return

            #парсинг рейтинга
            try:
                rat = soup.find_all('div', class_='flex flex-row')
                number_of_stars = rat[2].find_all('svg', class_='pr-1 styles_yellowStar__RI1fH')
                number_of_stars = len(number_of_stars)
                print(number_of_stars)

                #reviews = soup.find('div', class_ = 'flex flex-row gap-4 justify-center align-center')
                #print(reviews)
                number_of_reviews = soup.find('a', class_ = FIELDS[INDEX]['a'])
                #number_of_reviews = soup.find('div', class_='pr-1 styles_yellowStar__RI1fH')
                #number_of_reviews = soup.find_element_by_xpath('//*[@id="__next"]/div[2]/div[1]/div/div/div/div[1]/div[2]/div[3]/a')
                #print(number_of_reviews)
                number_of_reviews = re.findall(r'\d+', number_of_reviews.get_text(strip=True))
                if len(number_of_reviews) == 0:
                    number_of_reviews.append(0)
                print(number_of_reviews[0])
            except:

                number_of_reviews = [0]
                #return



            #получении главной ссылки компании
            href = soup.find('a', class_='styles_reset__1_PU9 styles_button__7X8Df styles_primary__ZcjWw styles_button__vE9cf')

            try:
                href = href.get('href')
                current_url = href
                if current_url[8:12] == 'play':
                    current_url = href
                else:
                    # проверка на защиту если есть то берем адресс с контактов, если нету то переходим по ссылки и берем адрес с адресной строки
                    if href == '#':
                        current_url = soup.find_all('td', class_='application-page-contact-left')
                        if current_url:
                            current_url = current_url[0].find('div')
                            current_url = current_url.get_text()[6::]
                        # если кнопка заблокирована и в контактах нет ссылки, то берем ее с адресной строки(не реализована, берем ее с функции get_content)
                        else:
                            current_url = button_protection_url
                    else:
                        # проверка на ссылку, если она точная сразу записываем, если нет то переходит по ссылки на сайт и парсим саму ссылку
                        # с адресной строки убирая все лишнее
                        if current_url[0:6] == 'https:' or current_url[0:5] == 'http:':
                            # провекра на лишние символы и то что идет после него (/, ?)
                            if current_url.find('t.me') or current_url.find('vk.cc'):
                                if current_url.count('?') > 0:
                                    current_url = '?'.join(current_url.split('?')[:-1])
                                    # print(current_url)
                            else:
                                if current_url.count('/') > 2:
                                    current_url = '/'.join(current_url.split('/')[:-1])
                                    # print(current_url)
                                if current_url.count('?') > 0:
                                    current_url = '?'.join(current_url.split('?')[:-1])
                                    # print(current_url)
                        else:
                            URL1 = URL_content + href
                            # print(URL1)
                            url = requests.get(URL1)
                            current_url = url.url
                            # print(current_url)

                        # проверка если после / какие либо символы, если есть подставляем их
                        try:

                            print(get_html(current_url + '/main').status_code)

                            if get_html(current_url + '/main').status_code == 200:

                                current_url = current_url + '/main'

                            if current_url.find('/app') > 0:

                                test_url = '/'.join(current_url.split('/')[:-1])

                                if get_html(current_url).status_code == 200:

                                    current_url = test_url

                        except:
                            pass
            except:
                current_url = '-'
                print(current_url)
            print(current_url)

            # получение ссылки от API и партнерской программы
            # подлючаемся к selenium

            task = asyncio.create_task(URL_API_affiliate(current_url))
            affiliate_api = await asyncio.gather(task)
            list.append([name, descrip, number_of_stars, number_of_reviews[0], current_url, affiliate_api[0][0],
                         affiliate_api[0][1], affiliate_api[0][2], affiliate_api[0][3]])
            print(list)
            print("writing_to_the_database")
            writing_to_the_database(list)
            print("end")
    except Exception as _ex:
        print(_ex)



async def URL_API_affiliate(html):
    global bool_api, html_api, bool_affiliate, html_affiliate
    #print(html+"222222222")

    current_url = html
    try:
        headers = {
            "User-Agent": "Mozilla / 5.0(Windows NT 10.0; Win64; x64) AppleWebKit "
                          "/ 537.36(KHTML, like Gecko) Chrome / 116.0 .5845 .686 YaBrowser / 23.9 .0 .0 Safari / 537.36"
        }

        timeout = ClientTimeout(total=None)  # 600)
        semaphore = asyncio.Semaphore(200)

        async with semaphore:

            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=5, force_close=True), timeout=timeout) as session2:

                async with session2.get(url=html, headers={"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}) as response1:
                    print('++++++++++++++++++++++++++++++++++++++++++++')
                    print(response1)
                    html_par = await response1.text()
                    soup1 = BeautifulSoup(html_par, 'html.parser')
                    items1 = soup1.find_all('a')
                    html_api = '-'
                    html_affiliate = '-'
                    bool_api = 'No'
                    bool_affiliate = 'No'
                    # поиск ссылки API  изменения ссылки по необходимости
                    for item in items1:
                        if 'API' in str(item.text):
                            if '/main' in current_url:
                                html_api = current_url.replace('/main', item.get('href'))
                                bool_api = 'Yes'

                            elif 'https' in item.get('href'):
                                html_api = item.get('href')
                                bool_api = 'Yes'

                            else:
                                html_api = current_url + item.get('href')
                                bool_api = 'Yes'

                        # поиск партнерской программы как у API
                        if 'Партнерская программа' in str(item.text) or 'Аффилиатная программа' in str(item.text):
                            if 'https' in item.get('href'):
                                html_affiliate = item.get('href')
                                bool_affiliate = 'Yes'

                            elif '/main' in current_url:
                                html_affiliate = current_url.replace('/main', item.get('href'))
                                bool_affiliate = 'Yes'

                            else:
                                html_affiliate = current_url + item.get('href')
                                bool_affiliate = 'Yes'
    except Exception as _ex:
        html_api = '--'
        html_affiliate = '--'
        bool_api = 'No'
        print(_ex)
        bool_affiliate = 'No'
    print(current_url, bool_affiliate, html_affiliate, bool_api, html_api)
    return bool_api, html_api, bool_affiliate, html_affiliate


async def get_content(html) :
    global content_url, button_protection_url

    options = Options()  # не ожидать полной прогрузки страницы сайта

    # options.page_load_strategy = 'none'
    options.page_load_strategy = 'eager'
    # options.page_load_strategy = 'normal'
    driver = webdriver.Chrome(options=options)
    #driver = webdriver.Chrome()

    driver.get(html)

    driver.maximize_window()

    lenpg = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);\
                                                  var lenpg=document.body.scrollHeight;return lenpg;")
    match = False

    # кролинг стр
    #time.sleep(2)
    while match == False:

        for i in range(len(FIELDS)):
            try:
                print( FIELDS[i]['button'])
                button = driver.find_element(By.XPATH, FIELDS[i]['button'])
                print(button)
            except Exception as _ex:
                print(_ex)
                continue
                #match = True

        # clicking on the button
            try:
                button.click()
            except Exception as _ex:
                print(_ex)

        lst = lenpg
        time.sleep(1.5)
        lenpg = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);\
                                                      var lenpg=document.body.scrollHeight;return lenpg;")

        if lst == lenpg:
            match = True

    html1 = driver.page_source
    #time.sleep(2)
    driver.close()

    soup = BeautifulSoup(html1, 'html.parser')


    #page = soup.find_all('div', class_='flex flex-column mb-10 sm:mb-15      mb-10 sm:mb-15 flex flex-column')
    page = soup.find_all('div', class_='mb-10 sm:mb-15 flex flex-column')

    print(page[0])
    timeout = ClientTimeout(total=None)#600)
    semaphore = asyncio.Semaphore(200)
    async with semaphore:

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=10, force_close=True), timeout=timeout) as session:

            tasks = []
            for item in page:

                item = item.find('a', class_='color-dark-grey fontSize-16 fontWeight-400')
                print(item.get('href'))
                f = open("test.txt", 'a')
                f.write(item.get('href') + '\n')
                f.close()
                task = asyncio.create_task(get_card(URL_content + item.get('href'), session))
                tasks.append(task)

            await asyncio.gather(*tasks)



#выбор категории
async def get_page(html) :

    driver = webdriver.Chrome()
    driver.get(html)
    driver.maximize_window()
    lenpg = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);\
                                          var lenpg=document.body.scrollHeight;return lenpg;")
    match = False
    # кролинг стр

    while match == False:

        lst = lenpg
        time.sleep(1.5)
        lenpg = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);\
                                              var lenpg=document.body.scrollHeight;return lenpg;")
        if lst == lenpg:

            match = True


    html1 = driver.page_source
    driver.close()
    soup = BeautifulSoup(html1, 'html.parser')
    #print(soup)
    items = soup.find_all('div', class_ = 'mb-10 sm:mb-16 flex flex-column gap-3')


    aaa= False
    for item in items:
        URL1 = item.find("a").get('href')
        #URL1 = item.find("a")
        f = open("test.txt", 'a')
        f.write(URL1 + '\n')
        f.close()
        print(URL1)
        print(URL_content + URL1)
        if aaa or "/categories/product-add-ons" in URL1:
            aaa = True
            task = asyncio.create_task(get_content(URL_content + URL1))
            await asyncio.gather(task)
        #asyncio.run(get_content(URL_content + URL1))


def parse() :
    asyncio.run(get_page(URL))
    #get_page(URL)

f = open("test.txt", 'w')
f.write('\n')
f.close()
#parse()

writing_to_the_excel()