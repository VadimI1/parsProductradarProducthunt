
import re
import time
from pprint import pprint

import apiclient
import gspread
import httplib2
import psycopg2
import requests
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

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
        # id = 1
        # for i in list:
        #     cursor.execute(
        #         'UPDATE the_company SET "ID" = %s WHERE "ID" = %s',
        #         (id, i[0]))
        #     id = id + 1
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
        if j % 30 == 0:
            time.sleep(30)
        j += 1

    # worksheet.update('A1', 'ID ')
    # worksheet.update('B1', 'Name')
    # worksheet.update('C1', 'Description')
    # worksheet.update('D1', 'Rating')
    # worksheet.update('E1', 'Number of reviews')
    # worksheet.update('F1', 'Link')
    # worksheet.update('G1', 'API')
    # worksheet.update('H1', 'Link API')
    # worksheet.update('I1', 'Affiliate Program')
    # worksheet.update('J1', 'Link Affiliate Program')



def writing_to_the_database(list):
    connection = None
    cursor = None
    try:

        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        print(f"Server version: {cursor.fetchone()}")


        # cursor.execute('CREATE TABLE "the_company" ('
        # '"ID"	serial primary key,'
        # '"Name"	TEXT,'
        # '"Description"	TEXT,'
        # '"Rating"	REAL,'
        # '"Number of reviews"	INTEGER,'
        # '"Link"	TEXT,'
        # '"API"	TEXT,'
        # '"Link API"	TEXT,'
        # '"Affiliate Program"	TEXT,'
        # '"Link Affiliate Program"	TEXT)')

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

        #если повторной записи нет, записываем в бд
        if flag:
            cursor.execute('INSERT INTO the_company ("Name", "Description", "Rating", "Number of reviews", "Link", "API", "Link API",'
                           '"Affiliate Program", "Link Affiliate Program") VALUES '
                           '(%s,%s,%s,%s,%s,%s,%s,%s,%s)',(i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8]))
        if flag_overwrites:
            cursor.execute('UPDATE the_company SET "Name" = %s, "Description" = %s, "Rating" = %s, "Number of reviews" = %s, '
                           '"Link" = %s, "API" = %s, "Link API" = %s,'
                           '"Affiliate Program" = %s, "Link Affiliate Program" = %s WHERE "ID" = %s', (i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8], id))
        cursor.execute('SELECT * FROM "the_company"')
        #print(cursor.fetchall())


    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("[INFO] PostgreSQL connection closed")

#####################################################
URL = 'https://startpack.ru/categories'
URL_content = 'https://startpack.ru'
HEADERS = {'user-agent' : 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/93.0.4577.82 YaBrowser/21.9.0.1044 Yowser/2.5 Safari/537.36',
           'accept': '*/*'}


current_url = ''
content_url = ''
button_protection_url = ''


def get_html(url, params = None) :
    r = requests.get(url, headers=HEADERS, params=params)
    return r




#парсинг информации о компании
def get_card(html):
    global current_url
    list = []

    soup = BeautifulSoup(html, 'html.parser')

    items = soup.find('div', class_='application-page-header-center')

    #парсинг названии компании
    name = items.find("h1").get_text()
    print(name)

    #парсинг описания
    descrip = soup.find('div', class_='editor-view-text')
    #print(descrip.get_text())

    #парсинг рейтинга
    rat = items.find('div', class_='application-page-ratings')
    number_of_stars = rat.find_all('span', class_='rating-star-24-full')
    number_of_stars = len(number_of_stars)

    number_of_half_stars = rat.find_all('span', class_='rating-star-24-half')
    if len(number_of_half_stars) > 0:
        number_of_stars = number_of_stars + 0.5
    #print(number_of_stars)

    number_of_reviews = re.findall(r'\d+', rat.get_text(strip=True))
    #print(number_of_reviews)
    if len(number_of_reviews) == 0:
        number_of_reviews.append(0)
    #print(number_of_reviews[0])

    #получении главной ссылки компании
    href = soup.find('div', class_='application-page-header-offsite')
    try:
        href = href.find('a').get('href')
        current_url = href
        if current_url[8:12] == 'play':
            current_url = href
        else:
            # print(current_url)
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
                # print(current_url)
                try:
                    if get_html(current_url + '/main').status_code == 200:
                        # if get_html(current_url).status_code != 200:
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

# получение ссылки от API и партнерской программы
    #подлючаемся к selenium
    try:
        options = Options()  # не ожидать полной прогрузки страницы сайта

        # options.page_load_strategy = 'none'
        options.page_load_strategy = 'eager'
        # options.page_load_strategy = 'normal'
        driver = webdriver.Chrome(options=options)
        driver.get(current_url)
        driver.maximize_window()
        time.sleep(0.5)
        lenpg = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);\
                                              var lenpg=document.body.scrollHeight;return lenpg;")
        time.sleep(0.5)
        match = False
        #кролинг стр
        while match == False:
            lst = lenpg
            #time.sleep(5)
            lenpg = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);\
                                          var lenpg=document.body.scrollHeight;return lenpg;")
            if lst == lenpg:
                match = True

        html = driver.page_source
        soup1 = BeautifulSoup(html, 'html.parser')
        items1 = soup1.find_all('a')
        html_api = '-'
        html_affiliate = '-'
        bool_api = 'No'
        bool_affiliate = 'No'
        #поиск ссылки API  изменения ссылки по необходимости
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

            #поиск партнерской программы как у API
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
    except:
        html_api = '-'
        html_affiliate = '-'
        bool_api = 'No'
        bool_affiliate = 'No'
        current_url = "-"


    list.append([name, descrip.get_text(strip=True), number_of_stars, number_of_reviews[0], current_url, bool_api,
                 html_api, bool_affiliate, html_affiliate])
    print(list)
    writing_to_the_database(list)




#выбор компании
def get_content(html) :
    global current_url, content_url, button_protection_url
    soup = BeautifulSoup(html, 'html.parser')
    page = soup.find('div', class_='pagination')
    page = page.find_all('a', class_='page-number')

    if len(page) != 0:
        print(page[len(page) - 1])
        for j in range(1, int(page[len(page) - 1].get_text())+1):
            print("str")
            print(j)
            URL2 = get_html(content_url + f'?page={j}')
            soup = BeautifulSoup(URL2.text, 'html.parser')
            items = soup.find_all('div', class_='category-description')
            items = items[1].find_all('div', class_='app-list-data-caption')
        #print(len(items))

            for i in range(len(items)):
                print("comp")
                print(i)
            #это в цикл и вытащить ссылки
                href = items[i].find('a')
                #print(href.get('href'))
                URL1 = URL_content + href.get('href')
                current_url = URL1
                button_protection_url = URL1
                URL1 = get_html(URL1)
                get_card(URL1.text)
                #items = items.find_all("li")
    else:
        items = soup.find_all('div', class_='category-description')
        items = items[1].find_all('div', class_='app-list-data-caption')
        href = items[0].find('a')
        URL1 = URL_content + href.get('href')
        current_url = URL1
        button_protection_url = URL1
        URL1 = get_html(URL1)
        get_card(URL1.text)



#выбор категории
def get_page(html) :
    global content_url
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find('div', class_ = 'category-menu category-menu-margin')
    items = items.find_all("li")
    #print(items[150])
    #это надо ручками цифры ставить 0, 18, 39, 47, 56, 66, 76, 91, 95, 104, 112, 117, 120, 127, 132, 137, 143, 147, 150
    for i in [143, 147, 150]:
        #print(i)
        href = items[i].find_all("a")
        #print(href[i].get('href'))
        URL1 = URL_content + href[0].get('href')
        content_url = URL1
        print(URL1)
        URL1 = get_html(URL1)
        get_content(URL1.text)

def parse() :

    html = get_html(URL)
    if html.status_code == 200:
        get_page(html.text)
    else:
        print ('Error')

#parse()
writing_to_the_excel()
