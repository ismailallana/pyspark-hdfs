# Calculate the top 5 movies per genre
# using groupByKey

from pyspark import SparkContext, SparkConf
from operator import add
import csv 
import argparse

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

# This function convert entries of ratings.csv into key,value pair of the following format
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

def naiveTopMovies(record):
    sortedRatingList = sorted(record, key=lambda rec:rec[1], reverse=True)
    return sortedRatingList[:5]

if __name__ == "__main__":
    sc = SparkContext(appName="Top Movies in Genre")
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", help="the output path", 
                        default='wk7_top5_naive/') 
    args = parser.parse_args()
    output_path = args.output

    ratings = sc.textFile("ratings.csv")
    movieData = sc.textFile("movies.csv")

    #(mid, count)
    movieRatingsCount = ratings.map(extractCount).reduceByKey(add)
    #(title,genre)
    movieGenre = movieData.flatMap(getGenre)

    #(genre,(title,count))
    genreRatings = movieGenre.join(movieRatingsCount).values().map(swapKey) 
    genreMovies = genreRatings.groupByKey(1)
    genreTop5Movies = genreMovies.mapValues(naiveTopMovies)
    genreTop5Movies.saveAsTextFile(output_path)

