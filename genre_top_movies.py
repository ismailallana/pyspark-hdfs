import csv
from operator import add

# This function convert entries of movies.csv into 
# key,value pair of the following format
# movieID -> (title,genre)
# since there may be multiple genre per movie, this function returns a list of key,value pair
def getGenre(record):
 
    for row in csv.reader([record]):
        if len(row) != 3:
            continue
        movieID, title, genres = row[0],row[1],row[2]
        return [(movieID, (title, genre)) for genre in genres.split("|")]

# This function convert entries of ratings.csv 
# into key,value pair of the following format
# movieID -> 1
def extractCount(record):
    try:
        userID, movieID, rating, timestamp = record.strip().split(",")
        return (movieID.strip(), 1)
    except:
        return ()
# This functions convert tuples of ((title, genre), number of rating) 
# into key,value pair of the following format
# genre -> (title, number of rating)
def swapKey(record):
    titleGenre, count = record
    title, genre = titleGenre
    return (genre, (title, count))

# This function is used by the aggregateByKey function to get local top 5 movies 
# It return a list of at most five records of (title, count) tuple sorted by count.

def mergeTopMovie(topMovieList, currentMovieCount):

    topList = sorted(topMovieList+[currentMovieCount], 
    key=lambda rec:rec[1], 
    reverse=True)
    return topList[:5]

# This function is used by the aggregateByKey function to global top 5 movies 
# from multiple local top 5
def mergeCombiners(topMovieList1, topMovieList2):
    topList = sorted(topMovieList1+topMovieList2, 
    key=lambda rec:rec[1], reverse=True)
    return topList[:5]

from pyspark import SparkConf, SparkContext
import argparse

if __name__ == "__main__":
  
    spark_conf = SparkConf()\
        .setAppName("Top Movie Per Genre")
    sc=SparkContext.getOrCreate(spark_conf) 

    parser = argparse.ArgumentParser()
    parser.add_argument("--output", help="the output path", 
                        default='week7_top55555') 
    args = parser.parse_args()
    output_path = args.output

    ratings = sc.textFile("ratings.csv")
    movieData = sc.textFile("movies.csv")
    movieRatingsCount = ratings.map(extractCount).reduceByKey(add)
    movieGenre = movieData.flatMap(getGenre)

    genreRatings = movieGenre.join(movieRatingsCount).values().map(swapKey)
    genreRatings.aggregateByKey([], 
		mergeTopMovie, 
		mergeCombiners, 1).saveAsTextFile(output_path)
    sc.stop()
