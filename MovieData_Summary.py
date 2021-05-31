from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, FloatType
import argparse

spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

parser = argparse.ArgumentParser()
parser.add_argument("--output", help="the output path",
                        default='week8_out')
args = parser.parse_args()
output_path = args.output

rating_data = 's3://comp5349-data/week6/ratings.csv'


rating_schema = StructType([
    StructField("uid", StringType(), True),
    StructField("mid", StringType(), True),
    StructField("rate", FloatType(), True),
    StructField("ts",IntegerType(),True)])

ratings = spark.read.csv(rating_data,header=False, schema=rating_schema)
ratings.describe(['rate']).show()


ratings.filter("mid<=5").groupBy('mid').avg('rate').show()

ratings.groupBy('mid').count().withColumnRenamed("count", "n").filter("n< 5").show()


from pyspark.sql.window import Window
from pyspark.sql.functions import count
window = Window.partitionBy("mid")
pop_rating = ratings.withColumn("n", count("mid").over(window)).filter("n > 100").drop("n").show()


movie_data = 's3://comp5349-data/week6/movies.csv'
movies_schema =  StructType([
    StructField("mid", StringType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)])

movies= spark.read.csv(movie_data,header=False, schema=movies_schema)

romance_1939=movies.filter(movies.title.contains("1939") & movies.genres.contains("Romance"))
print('All Romance Movies:')
romance_1939.show()
romance_1939.drop('genres') \
    .join(ratings,'mid','inner') \
    .drop('ts','uid') \
    .groupBy('title') \
    .avg('rate') \
    .coalesce(1) \
    .write.csv(output_path)
spark.stop()
