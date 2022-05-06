import tensorflow as tf
import os
import sys


def get_genre_count(path_to_images):
    num_dirs = 0
    for base, dirs, files in os.walk(path_to_images):
        for _ in dirs:
            num_dirs += 1
    return num_dirs


def create_data_generator():
    data_generator = tf.keras.preprocessing.image.ImageDataGenerator(
        shear_range=0.2,
        zoom_range=0.2,
        rotation_range=30,
        width_shift_range=0.1,
        height_shift_range=0.1,
        fill_mode='nearest',
        horizontal_flip=True,
        validation_split=0.3,
        rescale=1.0 / 255.0,
    )
    return data_generator


def create_train_data_flow(path_to_images, target_width, target_height, data_generator, batch_size):
    _train_data_flow = data_generator.flow_from_directory(
        directory=path_to_images,
        target_size=(target_width, target_height),
        batch_size=batch_size,
        class_mode='categorical',
        shuffle=True,
        color_mode='rgb',
        subset='training'
    )
    return _train_data_flow


def create_validation_data_flow(path_to_images, target_width, target_height, data_generator, batch_size):
    _validation_data_flow = data_generator.flow_from_directory(
        directory=path_to_images,
        target_size=(target_width, target_height),
        batch_size=batch_size,
        class_mode='categorical',
        shuffle=True,
        color_mode='rgb',
        subset='validation'
    )
    return _validation_data_flow


def build_model(num_genres):
    _model = tf.keras.Sequential()
    _model.add(tf.keras.layers.Conv2D(32, 3, padding="same", activation="relu", input_shape=(224, 224, 3)))
    _model.add(tf.keras.layers.MaxPool2D())

    _model.add(tf.keras.layers.Conv2D(32, 3, padding="same", activation="relu"))
    _model.add(tf.keras.layers.MaxPool2D())

    _model.add(tf.keras.layers.Conv2D(64, 3, padding="same", activation="relu"))
    _model.add(tf.keras.layers.MaxPool2D())
    _model.add(tf.keras.layers.Dropout(0.4))

    _model.add(tf.keras.layers.Flatten())
    _model.add(tf.keras.layers.Dense(128, activation="relu"))
    _model.add(tf.keras.layers.Dense(num_genres, activation="softmax"))

    _model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.00001),
                   loss='categorical_crossentropy',
                   metrics=['accuracy'])

    return _model


if __name__ == '__main__':
    data_generator_ = create_data_generator()
    path_to_images_ = '/airflow/data/images'
    num_genres_ = get_genre_count(path_to_images_)
    batch_size_ = 16
    image_size = 224
    train_data_flow = create_train_data_flow(path_to_images_, image_size, image_size, data_generator_, batch_size_)
    validation_data_flow = create_validation_data_flow(path_to_images_, image_size, image_size, data_generator_,
                                                       batch_size_)
    model = build_model(num_genres_)
    num_epochs = int(sys.argv[1])
    history = model.fit(
        train_data_flow,
        epochs=num_epochs,
        validation_data=validation_data_flow
    )
    model.save('/airflow/data/model.h5')
    print(history.history)
