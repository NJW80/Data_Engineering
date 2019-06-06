import urllib.request
from bs4 import BeautifulSoup
import math
import pandas as pd
import pandas.io.sql as sql
import json
import time
import random
from selenium import webdriver
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import Error
import re
from nome_functions import keys


def Get_Location_Details(location, sublocation):
    try:
        return location[sublocation]

    except KeyError as ke:
        return 'Unknown'

    except :
        return 'Unknown'


def Get_Opentable_City_Listing_Total(Opentable_Listing_URL):
    # Opentable_Listing_URL = "https://www.opentable.co.uk/london-restaurant-listings"
    page_response = urllib.request.urlopen(Opentable_Listing_URL).read()
    page_response=str(page_response)
    page_content = BeautifulSoup(page_response, "html.parser")

    n_results = int(page_content.find_all(attrs={'id': 'results-title'})[0].text.split()[0])
    n_pages = math.ceil(n_results / 100)
    # print(n_results)
    # print(n_pages)
    return n_pages


def Get_Opentable_URLs():
    Opentable_Listing_URL = "https://www.opentable.co.uk/london-restaurant-listings"
    n_pages = Get_Opentable_City_Listing_Total(Opentable_Listing_URL)

    # point Selenium to the GeckoDriver to create a headless Firefox browser
    driver = webdriver.Firefox(executable_path='C:\\Users\\User\\IdeaProjects\\PythonPrograms\\nome\\cleanse_places\\geckodriver.exe')

    # point the browser to the Opentable website for London Restaurant Listings
    driver.get(Opentable_Listing_URL)

    # create an empty list to store the restaurant name and Opentable URL data
    restaurant_URLS=[]

    # Loop through all of the pages for the city's restaurant listings
    for i in range(n_pages + 1):
        # sleep for a random number of seconds between 2 and 5 so as to not overload the Opentable webserver or
        # otherwise adversely impact their webserver
        time.sleep(random.randint(2,5))

        # read in the html source and have BeautifulSoup parse it so that we can extract the data needed
        # this step at the 1st level of the loop will reload each new pages after the 'Next' button is
        # 'clicked' by Selenium
        page_response=str(driver.page_source)
        page_content = BeautifulSoup(page_response, "html.parser")
        results = page_content.find_all('div', attrs={'class': 'rest-row-header'})

        for item in results:

            # loop through that pages restaurant listing and extract the name and Opentable URL
            restaurant_name = item.find('span', attrs={'class': 'rest-row-name-text'}).text
            restaurant_opentable_url = "https://www.opentable.co.uk" + item.find('a')['href'].split(sep='?')[0]
            restaurant_dict = {'restaurant_name': restaurant_name, 'restaurant_opentable_url': restaurant_opentable_url}

            # append the resulting dictionary to the list created outside of the for loops
            restaurant_URLS.append(restaurant_dict)

        # make Selenium 'click' the Next page button
        driver.execute_script("document.getElementsByClassName('pagination-next')[0].click()")

    # close the headless browser
    driver.quit()
    df = pd.DataFrame(restaurant_URLS)
    return df

import socket
def get_Opentable_Restaurant_Data(row):
    try:
        time.sleep(random.randint(1,3))
        page_response =  urllib.request.urlopen(row['restaurant_opentable_url'], timeout=5).read()
        # parse html
        page_response=str(page_response)
        page_content = BeautifulSoup(page_response, "html.parser")

        opentable_cuisine = page_content.find('span', attrs={'itemprop':'servesCuisine'}).text
        # print(opentable_cuisine)

        opentable_google_query = page_content.find('span', attrs={'itemprop':'streetAddress'}).text
        opentable_google_query = re.sub(' +', ' ', opentable_google_query.replace(',', ''))
        # print(opentable_google_query)

    except KeyError:
        opentable_cuisine = 'Unknown'
        opentable_google_query = 'Unknown'

    except socket.timeout:
        print("Timed out URL: {}".format(row['restaurant_opentable_url']))
        opentable_cuisine = 'Unknown'
        opentable_google_query = 'Unknown'

    except socket.gaierror:
        print("GAI Error URL: {}".format(row['restaurant_opentable_url']))
        opentable_cuisine = 'Unknown'
        opentable_google_query = 'Unknown'

    except:
        print("Unknown Error URL: {}".format(row['restaurant_opentable_url']))
        opentable_cuisine = 'Unknown'
        opentable_google_query = 'Unknown'

    finally:
        return opentable_cuisine, opentable_google_query


# https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=Museum%20of%20Contemporary%20Art%20Australia&inputtype=textquery&fields=photos,formatted_address,name,rating,opening_hours,geometry&key=xxx
# https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=Madison%201%20New%20Change%20London%2C%20%20%20EC4M%209AF&inputtype=textquery&fields=name,place_id,formatted_address&key=xxx

def get_google_place_data(row):
    try:
        time.sleep(random.randint(1,3))
        api_key = keys.get_google_api_key()
        query = row['restaurant_name'] + '%20' + row['Opentable_Address']
        cleaned_query = query.replace(' ', '%20')
        cleaned_query = cleaned_query.replace('&', 'and')
        cleaned_query = cleaned_query.replace("'", '')
        url = 'https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=' + cleaned_query + "&inputtype=textquery&fields=name,place_id,formatted_address" + '&key=' + api_key
        response = urllib.request.urlopen(url).read().decode('utf8')
        GooglePlaceData = json.loads(response)

        return GooglePlaceData['candidates'][0]['name'],GooglePlaceData['candidates'][0]['place_id'],GooglePlaceData['candidates'][0]['formatted_address']

    except UnicodeError as ue:
        print("UnicodeError on: {} -- {}".format(row['restaurant_name'], row['Opentable_Address']))
        print("Row: {}".format(row.name))
        print(url)
        print(ue)
        return "Unknown","Unknown","Unknown"

    except IndexError as ie:
        print("Index Error on: {} -- {}".format(row['restaurant_name'], row['Opentable_Address']))
        print("Row: {}".format(row.name))
        print(url)
        return "Unknown","Unknown","Unknown"

    except:
        print("Unknown Error on: {} -- {}".format(row['restaurant_name'], row['Opentable_Address']))
        print("Row: {}".format(row.name))
        print(url)
        return "Unknown","Unknown","Unknown"

# "https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=Afternoon%20tea%20at%20Hotel%20Café%20Royal%2068%20Regent%20Street%20London%20W1B%204DY&inputtype=textquery&fields=name,place_id,formatted_address&key=xxx"
#
# {
#     "candidates" : [
#         {
#             "formatted_address" : "10 Air St, Soho, London W1B 4DY, UK",
#             "name" : "Hotel Café Royal",
#             "place_id" : "ChIJawUNG9QEdkgRf4wGMuxqTXc"
#         },
#         {
#             "formatted_address" : "68 Regent St, Soho, London W1B 4DY, UK",
#             "name" : "Oscar Wilde Lounge",
#             "place_id" : "ChIJawUNG9QEdkgRpSczKlUHWu8"
#         }
#     ],
#     "status" : "OK"
# }



def write_places(data, tablename, replace_append):
    try:
        engine = create_engine("mysql://nick:{}@localhost/nome".format(keys.mysql_password()), convert_unicode=True, encoding="utf-8")
        con = engine.connect()

        data.to_sql(name=tablename, con=con, if_exists=replace_append)
        con.close()

    except Error as e:
        print(e)


def read_places(table):
    try:
        conn = mysql.connector.connect(host='localhost',  database='nome', user='nick', password=keys.mysql_password())

        query = "SELECT * FROM {}".format(table)
        df = pd.read_sql(query, conn)

    except Error as e:
        df = pd.DataFrame.empty
        print(e)

    finally:
        return df


# SELECT City, count(*) as n_Restaurants
# FROM nome.nome_places_data
# GROUP BY City
# ORDER BY n_Restaurants desc;