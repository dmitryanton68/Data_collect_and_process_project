# Учебный проект по сбору и анализу данных реализован в 2-х файлах:
# Parsing_project_by_Antonov.py и Data_analize_by_Antonov.py
# Данные парсятся с сайта imdb.com и взяты с сайта ilo.org (файл 'GDP.csv')
# -------------------------------------------------------------------------
# Файл 1
#--------------------------------------------------------------------------

import requests
from pymongo import MongoClient
from bs4 import BeautifulSoup

def film_data():
    '''
    Скачивание html-скрипта с перечнем фильмов с сайта IMDB
    '''
    url = 'https://www.imdb.com/chart/top'
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/53.0.2785.143 Safari/537.36'
    }
    try:
        request = requests.get(url,
                               params={'ref_': 'nv_mv_250'},
                               headers=headers)
        return request.text
    except requests.exceptions.ConnectionError:
        print("No connection to site")
        exit(1)

def save_data():
    '''
    Парсинг данных с сайта IMDB и сохранение в базе данных MongoDB
    '''
    client = MongoClient('mongodb://127.0.0.1:27017')
    db = client['films']
    films = db.films
    films.drop()

    soup = BeautifulSoup(film_data(), 'html.parser')
    film_list = soup.find("tbody", {"class": "lister-list"}).findAll("tr")

    if film_list:
        for film in film_list:
            film_number = int(film.find("td", {"class": "titleColumn"}).contents[0].split('\n')[1].split('.')[0])
            film_name = film.find("td", {"class": "titleColumn"}).find("a").string
            film_year = int((film.find("span", {"class": "secondaryInfo"}).string).split('(')[1].split(')')[0])
            film_rating = float(film.find("td", {"class": "ratingColumn imdbRating"}).find("strong").string)

            film_d = {"film_number": film_number,
                      "film_name": film_name,
                      "film_year": film_year,
                      "film_rating": film_rating}
            films.insert_one(film_d)
        return films
    else:
        print("At your request no results were found. Please, check your request.")
        film_data()
