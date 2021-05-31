spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    genre_top_movies_naive.py \
    --output $1 
