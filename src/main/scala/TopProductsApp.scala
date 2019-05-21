/** Spark application to calculate top products by session duration.
  * Definition of a session: <em>session lasts until the user is looking at particular product.
  * When particular user switches to another product the new session starts.</em>
  * Input example could be found at `data/example.csv`.
  *
  * Input format: `category,product,userId,eventTime,eventType,sessionStartTime,sessionEndTime,sessionId`
  *
  * Output format: `product,durationInSeconds`
  */
object TopProductsApp extends GenericApp {

  def appName = "top-products-app"

  def execute(inputPath: String, outputPath: String) = withSpark { spark =>
    val events = spark.read
      .option("header", "true")
      .csv(inputPath)

    events.createTempView("events")
    spark.udf.register("session_start", new SessionStartTime)

    val topProducts = spark.sql(
      """SELECT product,
        sum(unix_timestamp(cast(eventTime AS TIMESTAMP)) - unix_timestamp(sessionStartTime))
          AS durationInSeconds
        FROM (SELECT *, session_start(product, cast(eventTime AS TIMESTAMP))
          OVER (PARTITION BY userId ORDER BY cast(eventTime AS TIMESTAMP))
          AS sessionStartTime FROM events)
        GROUP BY product
        ORDER BY durationInSeconds DESC
        LIMIT 10""")
      .cache()

    topProducts.write
      .option("header", "true")
      .csv(outputPath)

    topProducts.show()
  }

  run()

}
