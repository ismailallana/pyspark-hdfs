from pyspark import SparkConf, SparkContext
from ml_utils import *
import argparse


if __name__ == "__main__":
    spark_conf = SparkConf()\
        .setAppName("Week 6 Lecture Sample Code")
    sc=SparkContext.getOrCreate(spark_conf)
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", help="the output path", 
                        default='rating_out_script') 
    args = parser.parse_args()
    output_path = args.output
    ratings = sc.textFile("ratings.csv")
    movieData = sc.textFile("movies.csv")
    movieRatings = ratings.map(extractRating)
    movieGenre = movieData.flatMap(pairMovieToGenre) # we use flatMap as there are multiple genre per movie
    genreRatings = movieGenre.join(movieRatings).values()
    genreRatingsAverage = genreRatings.aggregateByKey((0.0,0), seqFunc, combFunc, 1).mapValues(avg)
    genreRatingsAverage.saveAsTextFile(output_path)
