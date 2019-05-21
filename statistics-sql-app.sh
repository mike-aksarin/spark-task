if sbt package
then
    rm -r output/statistics

    spark-submit \
    --class StatisticsSqlApp \
    --master local[*] \
    target/scala-2.11/spark-task_2.11-0.1.jar \
    output/sessions output/statistics

fi
