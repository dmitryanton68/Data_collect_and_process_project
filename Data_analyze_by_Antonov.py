# Учебный проект по сбору и анализу данных реализован в 2-х файлах:
# Parsing_project_by_Antonov.py и Data_analize_by_Antonov.py
# Данные парсятся с сайта imdb.com и взяты с сайта ilo.org (файл 'GDP.csv')
# -------------------------------------------------------------------------
# Файл 2
#--------------------------------------------------------------------------

from pymongo import MongoClient
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from Parsing_project_by_Antonov import save_data

save_data()

client = MongoClient('mongodb://127.0.0.1:27017')
db = client.films
collection = db.films

def data_transform(collection=collection):
    '''
    Коллекция MongoDB для обработки трансформируется в pandas DataFrame
    '''
    data = pd.DataFrame(list(collection.find()), columns=['_id', 'film_number', 'film_name', 'film_year', 'film_rating'])
    data_rating = pd.DataFrame(data.groupby('film_year')['film_rating'].sum())
    data_rating.reset_index(inplace=True)
    return data_rating

def gdp_data(file='GDP.csv'):
    '''
    Данные о ВВП (GDP) стран мира взяты с сайта International Labour Organization (ILO)
    '''
    gdp = pd.read_csv(file, sep=',', encoding='latin1')
    gdp.drop(['Country Code', 'Indicator Name', 'Indicator Code'], axis=1, inplace=True)
    gdp_USA = gdp.loc[gdp['Country Name']=='United States', :].T
    gdp_USA.reset_index(inplace=True)
    gdp_USA = gdp_USA.drop([0, 60], axis=0)
    gdp_USA = gdp_USA.rename(columns={'index': 'film_year', 249: 'GDP'})
    # добавлены отсутствующие данные по 2018 году
    gdp_USA.loc[gdp_USA['film_year'] == '2018', 'GDP'] = 2.05134e+13
    gdp_USA = gdp_USA.astype('float64')
    return gdp_USA

def imagination(data_rating=data_transform(), gdp_USA=gdp_data(), n=10):
    '''
    Отображение на графике:
    1. суммарного рейтинга фильмов по годам
    2. среднего скользящего по суммарного рейтинга по годам
    3. отмасштабированного ВВП США (домножены на 3/10^12 для сопоставимости и приведения на едином графике)
    '''
    rolling_mean = data_rating['film_rating'].rolling(window=n).mean()
    plt.figure(figsize=(12,5))
    plt.plot(data_rating['film_year'], rolling_mean, "g", label='Скользящее среднее по рейтингам')
    plt.plot(data_rating['film_year'], data_rating['film_rating'], "b", label='Суммарный рейтинг за год')
    plt.plot(gdp_USA['film_year'], 3*gdp_USA['GDP']/(1e+12), "r", label='ВВП США')
    plt.legend(loc="lower right")
    plt.grid(5)
    plt.vlines(1975, 0, 70, color='silver', linewidth=5, linestyle='--')
    plt.vlines(2008, 0, 70, color='silver', linewidth=5, linestyle='--')

    plt.title(f"Корреляция суммарного рейтинга фильмов, выпускаемых мировыми 'фабриками грёз', "
              f"с динамикой изменения ВВП США \n (распределение суммарного рейтинга лучших 250 фильмов "
              f"по мнению сайта IMDB)")
    plt.xlabel('год')
    plt.ylabel('Суммарный рейтинг фильмов года')
    plt.show()

def data_correlation(data_rating=data_transform(), gdp_USA=gdp_data(), n=10):
    '''
    Расчёт ковариации и корреляции Пирсона
    '''
    X = data_rating['film_rating'].rolling(window=n).mean()[38:78].reset_index()
    Y = pd.DataFrame({'gdp': 3*gdp_USA['GDP'][15:55]/(1e+12)}, columns=['gdp']).reset_index()
    Z = data_rating['film_rating'][38:78].reset_index()

    cov_1 = np.cov(X, Y)
    c1 = cov_1[0, 1]

    cov_2 = np.cov(Z, Y)
    c2 = cov_2[0, 1]

    coeff_Pirson = np.corrcoef(X, Y)
    cp = coeff_Pirson[0, 1]

    return print(f'По данным парсинга рейтинга 250 наиболее популярных фильмов (сайт IMDB) '
                 f'в промежутке между 1975 и 2008 годом: \n '
                 f'1. ковариация скользящего среднего по суммарным рейтингам фильмов (по годам) '
                 f'и ВВП США составляет {c1:.{2}f}, \n '
                 f'2. ковариация суммарных рейтингов фильмов (по годам) и ВВП США составляет {c2:.{2}f}, \n'
                 f'3. коэффициент корреляции Пирсона между скользящим средним по суммарным рейтингам фильмов '
                 f'(по годам) и ВВП США составляет {cp:.{2}f}')

data_correlation()

print('-'*120)
print(f'P.S. Жёсткая корреляция между скользящим средним по суммарным рейтингам фильмов (по годам) и ВВП США \n'
      f'в промежутке между 1975 и 2008 годом и отсутствие таковой после 2008 года (см.граф.) '
      f'наводит на размышления...')

imagination()
