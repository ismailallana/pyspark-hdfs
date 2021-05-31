spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 3 \
    --py-files ml_utils.py AverageRatingPerGenre.py \
    --output $1 
