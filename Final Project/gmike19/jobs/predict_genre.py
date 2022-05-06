import os
import tensorflow as tf
import numpy as np
import sys

if __name__ == '__main__':
    poster_file = sys.argv[1]
    path_to_images = '/airflow/data/images/'
    labels = [c for c in os.listdir(path_to_images) if os.path.isdir(path_to_images + c)]
    model = tf.keras.models.load_model('/airflow/data/model.h5')
    to_predict_path = '/airflow/data/' + poster_file if not os.path.isabs(poster_file) else poster_file
    img_size = 224
    if os.path.isfile(to_predict_path):
        img = tf.keras.preprocessing.image.load_img(to_predict_path, target_size=(img_size, img_size))
        img_arr = tf.keras.preprocessing.image.img_to_array(img)
        img_arr = np.expand_dims(img_arr, axis=0)
        normalized_arr = np.vstack([img_arr]) / 255.0
        classes = model.predict(normalized_arr)

        print(f'\n{to_predict_path} most likely to be a {labels[np.argmax(classes)]} movie \n')
        print(f'all probabilities: ')
        print(dict(zip(labels, list(classes[0]))))
    else:
        print('No such file')
