from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import dataframe
from pyspark.sql.types import *


def get_movies_metadata():
    movies_schema = StructType([StructField("adult", BooleanType(), True),
                                StructField("belongs_to_collection", StringType(), True),
                                StructField("budget", IntegerType(), True),
                                StructField("genres", StringType(), True),
                                StructField("homepage", StringType(), True), StructField("id", IntegerType(), True),
                                StructField("imdb_id", StringType(), True),
                                StructField("original_language", StringType(), True),
                                StructField("original_title", StringType(), True),
                                StructField("overview", StringType(), True),
                                StructField("popularity", DoubleType(), True),
                                StructField("poster_path", StringType(), True),
                                StructField("production_companies", StringType(), True),
                                StructField("production_countries", StringType(), True),
                                StructField("release_date", DateType(), True),
                                StructField("revenue", IntegerType(), True),
                                StructField("runtime", DoubleType(), True),
                                StructField("spoken_languages", StringType(), True),
                                StructField("status", StringType(), True),
                                StructField("tagline", StringType(), True), StructField("title", StringType(), True),
                                StructField("video", BooleanType(), True),
                                StructField("vote_average", DoubleType(), True),
                                StructField("vote_count", IntegerType(), True)
                                ])

    movies_whole = spark \
        .read \
        .option('inferSchema', 'false') \
        .option("multiLine", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("wholeFile", True) \
        .csv(f'{data_lake_path}/archive/movies_metadata.csv', header=True, schema=movies_schema)

    movies_whole = movies_whole.where("imdb_id not in ('0', 'tt0113002', 'tt2423504', 'tt2622826')")
    movies_whole = movies_whole.withColumn('imdb_id', f.expr('substring(imdb_id, 3)'))

    return movies_whole


def replace_for_json_characters(df: dataframe.DataFrame, col_name: str) -> dataframe.DataFrame:
    df = df.withColumn(col_name, f.regexp_replace(f.col(col_name), ': None', ': null')) \
        .withColumn(col_name, f.regexp_replace(f.col(col_name), "\\\\", ""))
    return df


def json_col_to_arraytype(df: dataframe.DataFrame, col_name: str, schema: ArrayType):
    df = replace_for_json_characters(df, col_name)
    df = df.withColumn(col_name, f.from_json(df[col_name], schema))
    return df


def transform_load_companies(movies_whole: dataframe.DataFrame):
    companies_schema = ArrayType(StructType([StructField('name', StringType(), True),
                                             StructField('id', IntegerType(), True)]))
    movies_whole = json_col_to_arraytype(movies_whole, 'production_companies', companies_schema)

    companies_whole = movies_whole \
        .withColumn('companies', f.explode('production_companies')) \
        .select('id',
                f.col('companies.id').alias('company_id'),
                f.col('companies.name').alias('company_name')
                )
    companies_only = companies_whole.select('company_id', 'company_name').distinct()
    companies_movies_bridge = companies_whole.select(f.col('id').alias('movie_id'), 'company_id')
    companies_only.write.option('path', f'{data_lake_path}/companies_ext') \
        .mode('overwrite').saveAsTable('movies.companies_ext', format='parquet')
    companies_movies_bridge.write.option('path', f'{data_lake_path}/companies_movies_ext') \
        .mode('overwrite').saveAsTable('movies.companies_movies_ext', format='parquet')


def transform_load_countries(movies_whole: dataframe.DataFrame):
    countries_schema = ArrayType(StructType([StructField('iso_3166_1', StringType(), True),
                                             StructField('name', StringType(), True)]))
    movies_whole = json_col_to_arraytype(movies_whole, 'production_countries', countries_schema)

    countries_whole = movies_whole \
        .withColumn('countries', f.explode('production_countries')) \
        .select('id',
                f.col('countries.iso_3166_1').alias('country_id'),
                f.col('countries.name').alias('country_name')
                )
    countries_only = countries_whole.select('country_id', 'country_name').distinct()
    countries_movies_bridge = countries_whole.select(f.col('id').alias('movie_id'), 'country_id')
    countries_only.write.option('path', f'{data_lake_path}/countries_ext') \
        .mode('overwrite').saveAsTable('movies.countries_ext', format='parquet')
    countries_movies_bridge.write.option('path', f'{data_lake_path}/countries_movies_ext') \
        .mode('overwrite').saveAsTable('movies.countries_movies_ext', format='parquet')


def transform_load_languages(movies_whole: dataframe.DataFrame):
    languages_schema = ArrayType(StructType([StructField('iso_639_1', StringType(), True),
                                             StructField('name', StringType(), True)]))
    movies_whole = json_col_to_arraytype(movies_whole, 'spoken_languages', languages_schema)

    languages_whole = movies_whole \
        .withColumn('languages', f.explode('spoken_languages')) \
        .select('id',
                f.col('languages.iso_639_1').alias('language_id'),
                f.col('languages.name').alias('language_name')
                )
    languages_only = languages_whole.select('language_id', 'language_name').distinct()
    languages_movies_bridge = languages_whole.select(f.col('id').alias('movie_id'), 'language_id')
    languages_only.write.option('path', f'{data_lake_path}/languages_ext') \
        .mode('overwrite').saveAsTable('movies.languages_ext', format='parquet')
    languages_movies_bridge.write.option('path', f'{data_lake_path}/languages_movies_ext') \
        .mode('overwrite').saveAsTable('movies.languages_movies_ext', format='parquet')


def transform_load_genres(movies_whole: dataframe.DataFrame):
    genres_schema = ArrayType(StructType([StructField('id', IntegerType(), True),
                                          StructField('name', StringType(), True)]))
    movies_whole = json_col_to_arraytype(movies_whole, 'genres', genres_schema)

    genres_whole = movies_whole \
        .withColumn('genres_exp', f.explode('genres')) \
        .select('id',
                f.col('genres_exp.id').alias('genre_id'),
                f.col('genres_exp.name').alias('genre_name')
                )
    genres_only = genres_whole.select('genre_id', 'genre_name').distinct()
    genres_movies_bridge = genres_whole.select(f.col('id').alias('movie_id'), 'genre_id')
    genres_only.write.option('path', f'{data_lake_path}/genres_ext') \
        .mode('overwrite').saveAsTable('movies.genres_ext', format='parquet')
    genres_movies_bridge.write.option('path', f'{data_lake_path}/genres_movies_ext') \
        .mode('overwrite').saveAsTable('movies.genres_movies_ext', format='parquet')


def transform_load_actors():
    credits_schema = StructType([
        StructField("cast", StringType(), True),
        StructField("crew", StringType(), True),
        StructField("id", IntegerType(), True)
    ])

    cast_schema = ArrayType(StructType([StructField('cast_id', IntegerType(), True),
                                        StructField('character', StringType(), True),
                                        StructField('credit_id', StringType(), True),
                                        StructField('gender', IntegerType(), True),
                                        StructField('id', IntegerType(), True),
                                        StructField('name', StringType(), True),
                                        StructField('order', IntegerType(), True),
                                        StructField('profile_path', StringType(), True)]))

    creds = spark \
        .read \
        .option('inferSchema', 'false') \
        .option("multiLine", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("wholeFile", True) \
        .csv(f'{data_lake_path}/archive/credits', header=True, schema=credits_schema)
    creds = json_col_to_arraytype(creds, 'cast', cast_schema)
    creds_whole = creds \
        .withColumn('member', f.explode('cast')) \
        .select('id', f.col('member.id').alias('actor_id'), 'member.name', 'member.character')
    actors_only = creds_whole.select('actor_id', 'name').distinct()
    actors_movies_bridge = creds_whole.select(f.col('id').alias('movie_id'), 'actor_id', 'character')
    actors_only.write.option('path', f'{data_lake_path}/actors_ext') \
        .mode('overwrite').saveAsTable('movies.actors_ext', format='parquet')
    actors_movies_bridge.write.option('path', f'{data_lake_path}/actors_movies_ext') \
        .mode('overwrite').saveAsTable('movies.actors_movies_ext', format='parquet')


def transform_load_keywords():
    keywords_csv_schema = StructType([StructField("id", IntegerType(), True),
                                      StructField("keywords", StringType(), True)
                                      ])

    keywords_schema = ArrayType(StructType([StructField('id', IntegerType(), True),
                                            StructField('name', StringType(), True)]))

    keywords = spark \
        .read \
        .option('inferSchema', 'false') \
        .option("multiLine", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("wholeFile", True) \
        .csv(f'{data_lake_path}/archive/keywords.csv', header=True, schema=keywords_csv_schema)

    keywords = json_col_to_arraytype(keywords, 'keywords', keywords_schema)

    keywords_whole = keywords \
        .withColumn('kw', f.explode('keywords')) \
        .select('id', f.col('kw.id').alias('keyword_id'), 'kw.name')
    keywords_only = keywords_whole.select('keyword_id', 'name').distinct()
    keywords_movies_bridge = keywords_whole.select(f.col('id').alias('movie_id'), 'keyword_id')
    keywords_only.write.option('path', f'{data_lake_path}/keywords_ext') \
        .mode('overwrite').saveAsTable('movies.keywords_ext', format='parquet')
    keywords_movies_bridge.write.option('path', f'{data_lake_path}/keywords_movies_ext') \
        .mode('overwrite').saveAsTable('movies.keywords_movies_ext', format='parquet')


def transform_load_ratings() -> dataframe.DataFrame:
    ratings_csv_schema = StructType([StructField("userId", IntegerType(), True),
                                     StructField("movieId", IntegerType(), True),
                                     StructField("rating", DoubleType(), True),
                                     StructField("timestamp", IntegerType(), True)
                                     ])

    ratings = spark \
        .read \
        .option('inferSchema', 'false') \
        .option("multiLine", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("wholeFile", True) \
        .csv(f'{data_lake_path}/archive/ratings', header=True, schema=ratings_csv_schema)

    ratings = ratings.select('userId', 'movieId', 'rating')

    votes = ratings.groupby('movieId').agg(f.avg('rating').alias('vote_average'),
                                           f.count('rating').alias('vote_count'))
    ratings.write.option('path', f'{data_lake_path}/ratings_ext').mode('overwrite') \
            .saveAsTable('movies.ratings_ext', format='parquet')
    return votes


def finalize_load_movies_table(movies_whole: dataframe.DataFrame, votes: dataframe.DataFrame):
    movies_final = movies_whole.select('id',
                                       'imdb_id',
                                       'homepage',
                                       'original_title',
                                       'overview',
                                       'popularity',
                                       'release_date',
                                       'revenue',
                                       'runtime',
                                       'status',
                                       'title')
    movies_final = movies_final.join(votes, votes.movieId == movies_final.id, 'left').drop('movieId')
    movies_final = movies_final.withColumn('vote_count', movies_final.vote_count.cast('int'))
    movies_final.write.option('path', f'{data_lake_path}/movies_table_ext') \
        .mode('overwrite').saveAsTable('movies.movies_table_ext', format='parquet')


if __name__ == '__main__':
    app_name = 'DataEngineering'

    conf = SparkConf()

    hdfs_host = 'hdfs://namenode:8020'

    conf.set("hive.metastore.uris", "http://hive-metastore:9083")
    conf.set("spark.kerberos.access.hadoopFileSystem", hdfs_host)
    conf.set("spark.sql.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")
    conf.set("hive.metastore.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")
    conf.setMaster("local[*]")

    data_lake_path = f'{hdfs_host}/DataLake'
    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql(f"create schema if not exists movies location '{data_lake_path}/movies_schema'")
    movies_whole_ = get_movies_metadata()
    transform_load_companies(movies_whole_)
    transform_load_countries(movies_whole_)
    transform_load_languages(movies_whole_)
    transform_load_genres(movies_whole_)
    transform_load_actors()
    transform_load_keywords()
    votes_ = transform_load_ratings()
    finalize_load_movies_table(movies_whole_, votes_)
