import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


def fetch_df_from_hdfs(poster_count_limit):
    df = spark.sql(f"""
                        select
                            m.imdb_id,
                            ge.genre_name
                        from movies.movies_table_ext m 
                        join movies.genres_movies_ext g on g.movie_id = m.id
                        join movies.genres_ext ge on ge.genre_id = g.genre_id
                        limit {poster_count_limit}
                        """)
    return df.toPandas()


def write_df_to_data_folder(df):
    df.set_index('imdb_id').to_csv('/airflow/data/movie_genres.csv', header=False)


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

    write_df_to_data_folder(fetch_df_from_hdfs(sys.argv[1]))
