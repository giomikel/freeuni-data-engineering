import os
import csv
import urllib.request
from urllib.error import ContentTooShortError
import shutil
import sys
from imdb import IMDb


def url_download(url, destination_path):
    try:
        urllib.request.urlretrieve(url, destination_path)
    except ContentTooShortError:
        print('Retrieval incomplete, trying again')
        url_download(url, destination_path)


def download_movie_posters(start_index, end_index):
    with open('/airflow/data/movie_genres.csv', newline='') as f:  # Fetch movie_genres csv file
        reader = csv.reader(f)
        genres_as_list = list(reader)
    genres_as_list = genres_as_list[start_index: end_index]
    # Dictionary for keeping track of already downloaded posters
    # In case of multiple genres posters get copied to other directories
    # instead of downloading it again
    downloaded_id_path_dict = dict()
    _imdb = IMDb()
    images_path = '/airflow/data/images'  # Path to images directory where posters will be stored
    for imdb_id, genre in genres_as_list:  # For each imdb_id-genre pair in list
        url = _imdb.get_movie(imdb_id).get_fullsizeURL()  # Get image URL
        if url:  # If URL to poster exists
            image_extension = url[-3:]  # Get image extension, each one ive seen was jpg but just in case its png
            destination_folder = images_path + '/' + genre  # Path to corresponding genre folder
            image_path = destination_folder + '/' + imdb_id + '.' + image_extension  # Path to write image to
            if not os.path.exists(destination_folder):  # If current genre folder doesn't exist
                os.makedirs(destination_folder)  # Create the folder, otherwise theres path not found error
            if imdb_id in downloaded_id_path_dict:  # If the poster has already been downloaded
                shutil.copy(downloaded_id_path_dict[imdb_id],
                            image_path)  # Copy the poster to current genre directory
            else:
                url_download(url, image_path)  # Download the image
                downloaded_id_path_dict[imdb_id] = image_path  # Write downloaded image path to dictionary


if __name__ == '__main__':
    task_number = int(sys.argv[1])
    indices_str = sys.argv[2:]
    current_indices = indices_str[task_number * 2: task_number * 2 + 1 + 1]
    start_index_ = int(current_indices[0])
    end_index_ = int(current_indices[1])
    download_movie_posters(start_index=start_index_, end_index=end_index_)
