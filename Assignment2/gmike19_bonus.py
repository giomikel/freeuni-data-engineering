from google.cloud import bigquery
from google.cloud.bigquery import ExternalSourceFormat
from google.cloud.exceptions import NotFound

client = bigquery.Client()
bucket_name = 'gmike19'
my_cloud_storage_prefix = 'gs://' + bucket_name + '/'
path_to_users_csv = 'users.csv'
path_to_ratings_csv = 'ratings.csv'
path_to_tags_csv = 'tags.csv'
path_to_movies_csv = 'movies.csv'
path_to_exported = 'exported/'
export_prefix = my_cloud_storage_prefix + path_to_exported + client.project

print(client.project)

dataset_location = 'US'


def create_dataset(dataset_id):
    dataset_id = '{}.{}'.format(client.project, dataset_id)

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = dataset_location
    dataset = client.create_dataset(dataset_id)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


def update_dataset_description(dataset_id, new_description):
    dataset = client.get_dataset(dataset_id)  # Make an API request.
    dataset.description = new_description
    dataset = client.update_dataset(dataset, ["description"])  # Make an API request.

    full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
    print(
        "Updated dataset '{}' with description '{}'.".format(
            full_dataset_id, dataset.description
        )
    )


def dataset_exists(dataset_id):
    try:
        client.get_dataset(dataset_id)  # Make an API request.
        print("Dataset {} already exists".format(dataset_id))
    except NotFound:
        print("Dataset {} is not found".format(dataset_id))


def delete_dataset(dataset_id, cascade=False):
    # Use the delete_contents parameter to delete a dataset and its contents.
    # Use the not_found_ok parameter to not receive an error if the dataset has already been deleted.
    client.delete_dataset(
        dataset_id, delete_contents=cascade, not_found_ok=True
    )  # Make an API request.

    print("Deleted dataset '{}'.".format(dataset_id))


def create_native_table(dataset_id, table_id, schema=None):
    full_table_id = '.'.join([client.project, dataset_id, table_id])
    table = bigquery.Table(full_table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, dataset_id, table.table_id)
    )


def create_native_table_from_query(dataset_id, table_id, query):
    table_id = '.'.join([client.project, dataset_id, table_id])

    job_config = bigquery.QueryJobConfig(destination=table_id)

    # Start the query, passing in the extra configuration.
    query_job = client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(table_id))


def create_external_table(dataset_id,
                          table_id,
                          schema=None,
                          source_paths=[],
                          source_format=ExternalSourceFormat.PARQUET):
    full_table_id = '.'.join([client.project, dataset_id, table_id])

    external_config = bigquery.ExternalConfig(source_format=source_format)
    external_config.source_uris = source_paths
    external_config.schema = schema

    table = bigquery.Table(full_table_id)
    table.external_data_configuration = external_config

    client.create_table(table)


def drop_table(dataset_id, table_id):
    full_table_id = '.'.join([client.project, dataset_id, table_id])

    client.delete_table(full_table_id)

    print('Deleted table {}'.format(full_table_id))


def load_csv_into_table(dataset_id, table_id, uri, schema=None):
    full_table_id = '.'.join([client.project, dataset_id, table_id])
    job_config = bigquery.LoadJobConfig(schema=schema, source_format=bigquery.SourceFormat.CSV)
    load_job = client.load_table_from_uri(uri, full_table_id, job_config=job_config)
    load_job.result()
    destination_table = client.get_table(full_table_id)
    print('Loaded {} rows.'.format(destination_table.num_rows))


def display_query(sql):
    data = client.query(sql)
    for row in data.result():
        print(row)


def export_table(dataset_id, table_id):
    full_table_id = '.'.join([client.project, dataset_id, table_id])
    destination_uri = 'gs://{}/{}/{}.{}'.format(bucket_name, 'exported', full_table_id, 'parquet')
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.ExtractJobConfig(destination_format='PARQUET')

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="US",
        job_config=job_config
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(client.project, dataset_id, table_id, destination_uri)
    )


if __name__ == '__main__':
    staging_dataset_id = 'staging'
    movies_dataset_id = 'movies'

    staging_users_table_name = 'users'
    staging_ratings_table_name = 'ratings'
    staging_tags_table_name = 'tags'
    staging_movies_table_name = 'movies'

    create_dataset(staging_dataset_id)
    create_dataset(movies_dataset_id)
    staging_users_schema = [
        bigquery.SchemaField('user_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('first_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('last_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('birth_date', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('country', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('registration_date', 'STRING', mode='REQUIRED')
    ]
    create_native_table(staging_dataset_id, staging_users_table_name, staging_users_schema)
    load_csv_into_table(staging_dataset_id, staging_users_table_name, my_cloud_storage_prefix + path_to_users_csv)

    staging_ratings_schema = [
        bigquery.SchemaField('user_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('movie_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('rating', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('created_at', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('empty', 'STRING')  # ფაილში ხაზების ბოლოს მძიმე ეწერა და ერორს აგდებდა, 5 სვეტი ეგონა
    ]
    create_native_table(staging_dataset_id, staging_ratings_table_name, staging_ratings_schema)
    load_csv_into_table(staging_dataset_id, staging_ratings_table_name, my_cloud_storage_prefix + path_to_ratings_csv)

    staging_tags_schema = [
        bigquery.SchemaField('user_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('movie_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('tag', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('created_at', 'STRING', mode='REQUIRED')
    ]
    create_native_table(staging_dataset_id, staging_tags_table_name, staging_tags_schema)
    load_csv_into_table(staging_dataset_id, staging_tags_table_name, my_cloud_storage_prefix + path_to_tags_csv)

    staging_movies_schema = [
        bigquery.SchemaField('movie_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('title', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('genres', 'STRING', mode='REQUIRED')
    ]
    create_native_table(staging_dataset_id, staging_movies_table_name, staging_movies_schema)
    load_csv_into_table(staging_dataset_id, staging_movies_table_name, my_cloud_storage_prefix + path_to_movies_csv)

    staging_users_modified_table_name = 'users_modified'
    staging_users_modified_query = """
        SELECT
            user_id as user_id,
            first_name as first_name,
            last_name as last_name,
            parse_date('%m/%d/%Y', birth_date) as birth_date,
            country as country,
            parse_date('%m/%d/%Y', registration_date) as registration_date,
            extract(YEAR FROM parse_date('%m/%d/%Y', registration_date)) as registration_year
        FROM staging.users"""
    create_native_table_from_query(staging_dataset_id, staging_users_modified_table_name,
                                   staging_users_modified_query)

    export_table(staging_dataset_id, staging_users_modified_table_name)
    movies_users_table_name = 'users'
    create_external_table(movies_dataset_id, movies_users_table_name, source_paths=[
        export_prefix + '.' + staging_dataset_id + '.' + staging_users_modified_table_name + '.parquet'])
    # string-ზე partitioning ვერ ვნახე ვერსად :/

    staging_tags_modified_table_name = 'tags_modified'
    staging_tags_modified_query = """
        SELECT
            user_id as user_id,
            movie_id as movie_id,
            FORMAT("%T", ARRAY_AGG(tag)) as tag_list
        FROM staging.tags
        GROUP BY user_id, movie_id
    """
    create_native_table_from_query(staging_dataset_id, staging_tags_modified_table_name, staging_tags_modified_query)
    export_table(staging_dataset_id, staging_tags_modified_table_name)

    movies_tags_table_name = 'tags'
    create_external_table(movies_dataset_id, movies_tags_table_name, source_paths=[
        export_prefix + '.' + staging_dataset_id + '.' + staging_tags_modified_table_name + '.parquet'])

    staging_ratings_modified_table_name = 'ratings_modified'
    staging_ratings_modified_query = """
        SELECT
            user_id as user_id,
            movie_id as movie_id,
            cast(rating as FLOAT64) as rating,
            parse_timestamp('%m/%d/%Y %H:%M:%S', created_at) as created_at
        FROM staging.ratings
    """
    create_native_table_from_query(staging_dataset_id, staging_ratings_modified_table_name,
                                   staging_ratings_modified_query)

    movies_ratings_table_name = 'ratings'
    export_table(staging_dataset_id, staging_ratings_modified_table_name)
    create_external_table(movies_dataset_id, movies_ratings_table_name, source_paths=[
        export_prefix + '.' + staging_dataset_id + '.' + staging_ratings_modified_table_name + '.parquet'])

    staging_movies_split_table_name = 'movies_split'
    staging_movies_split_query = """
        SELECT
            movie_id as movie_id,
            concat(IF(instr(title, ':') != 0, substr(title, instr(title, ':') + 1,
            (length(substr(title, instr(title, ':') + 2)) - 6)), ''), substr(title, 0, length(title)
            - IF(instr(title, ':') != 0, length(substr(title, instr(title, ':'))), 6))) as title,
            safe_cast(substr(title, length(title) - 4, 4) as INTEGER) as year,
            split(genres, '|') as genre
        FROM staging.movies ORDER BY movie_id
    """
    create_native_table_from_query(staging_dataset_id, staging_movies_split_table_name, staging_movies_split_query)

    staging_genre_table_name = 'genres'
    staging_genre_table_query = """
        SELECT
            cast(rand() * 10000000 as int) as random_key,
            genr
        FROM staging.movies_split,
        UNNEST(movies_split.genre) as genr
        GROUP BY genr;
    """
    create_native_table_from_query(staging_dataset_id, staging_genre_table_name, staging_genre_table_query)
    export_table(staging_dataset_id, staging_genre_table_name)

    movies_genre_table_name = 'genres'
    create_external_table(movies_dataset_id, movies_genre_table_name, source_paths=[
        export_prefix + '.' + staging_dataset_id + '.' + staging_genre_table_name + '.parquet'
    ])

    staging_movies_final_name = 'movies_final'
    staging_movies_final_query = """
        SELECT
            m.movie_id as movie_id,
            m.title as title,
            m.year as year,
            FORMAT("%T", ARRAY_AGG(g.random_key)) as genre_ids
        FROM staging.movies_split as m, unnest(m.genre) as genr
        JOIN movies.genres g ON genr = g.genr
        GROUP BY m.movie_id, m.title, m.year
    """
    create_native_table_from_query(staging_dataset_id, staging_movies_final_name, staging_movies_final_query)
    export_table(staging_dataset_id, staging_movies_final_name)

    movies_movie_table_name = 'movies'
    create_external_table(movies_dataset_id, movies_movie_table_name, source_paths=[
        export_prefix + '.' + staging_dataset_id + '.' + staging_movies_final_name + '.parquet'
    ])
