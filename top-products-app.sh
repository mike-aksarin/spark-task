# Replace data/example.csv with your input path for a different dataset

if sbt package
then
    rm -r output/top_products

    spark-submit \
    --class TopProductsApp \
    --master local[*] \
    target/scala-2.11/spark-task_2.11-0.1.jar \
    data/example.csv output/top_products

fi
