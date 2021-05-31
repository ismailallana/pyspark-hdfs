spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    genre_top_movies.py \
    --output $1 
