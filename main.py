import json
import urllib.request
import pymongo
from bs4 import BeautifulSoup
import pandas as pd
import time
import re


def save_current_file(file_text, filetype='json'):
    current_page_out = open('output.{}'.format(filetype), 'w+')
    current_page_out.write(file_text)
    current_page_out.close()


def scrape_review_page(movie_id):
    movie_review_url = 'https://www.imdb.com/title/tt{}/reviews/'.format(str(movie_id).zfill(7))
    current_page = urllib.request.urlopen(movie_review_url)
    soup = BeautifulSoup(current_page, 'html.parser')

    movie_title = soup.select('meta [property="og:title"]')[0].get('content')
    current_movie_reviews = []

    for tag in soup.find_all(attrs={'class': "imdb-user-review"}):
        try:
            user_link = tag.select('.display-name-link a')[0].get('href')
            user_id = re.findall("/user/ur\d+", user_link)[0][8:]
            user_rating = tag.select('.rating-other-user-rating span')[0].text

            current_movie_reviews.append({
                "movie_id": movie_id,
                "user_id": user_id,
                "user_rating": user_rating,
                "title": movie_title,
            })
        except Exception as e:
            print("Error caused by movie_id : {}. Exception: {}".format(movie_id, e))
    return current_movie_reviews


def scrape_index_page(movie_id):
    movie_index_url = 'https://www.imdb.com/title/tt{}/'.format(str(movie_id).zfill(7))
    current_page = urllib.request.urlopen(movie_index_url)
    index_soup = BeautifulSoup(current_page, 'html.parser')
    current_page_json = index_soup.find('script', attrs={'type': 'application/ld+json'}).text
    return current_page_json


def make_connection(collection_name):
    mongodb_server = "localhost"
    mongodb_port = 27017
    mongodb_db = "imdb"
    mongodb_collection = collection_name
    connection = pymongo.MongoClient(mongodb_server, mongodb_port)
    db = connection[mongodb_db]
    collection = db[mongodb_collection]
    return collection


def save_current_instance(collection, current_page_json):
    collection.insert_one(current_page_json)


def get_movie_ids(num=500):
    links_data = pd.read_csv('./data/links.csv')
    movie_ids = list(links_data.imdbId)
    return movie_ids[:num]


def scrape_and_store_mongodb():
    print("Starting Scraping")
    print("=" * 20)
    start = time.time()
    num_movies = 200
    current_index = 0
    movie_ids = get_movie_ids(num=num_movies)
    movies_collection = make_connection(collection_name='movies')
    reviews_collection = make_connection(collection_name='reviews')
    for movie_id in movie_ids:
        movies_collection.insert_one(json.loads(scrape_index_page(movie_id)))
        reviews_collection.insert_many(scrape_review_page(movie_id))
        current_index = current_index + 1
        print("Movies completed: {}/{}".format(current_index, num_movies))
    print("Time taken: {}".format(time.time() - start))
    print("=" * 20)


# scrape_and_store_mongodb()
if __name__ == '__main__':
    collection = make_connection(collection_name='reviews')

    data = pd.DataFrame(list(collection.find()))
    print(data)
