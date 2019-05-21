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

  /** Top size */
  def limit: Int = 10

  def execute(inputPath: String, outputPath: String) = withSpark { spark =>
    val events = spark.read
      .option("header", "true")
      .csv(inputPath)

    events.createTempView("events")

    val eventTime = "unix_timestamp(cast(eventTime AS TIMESTAMP))"

    val productWindow =
      s"""OVER (PARTITION BY userId, product ORDER BY $eventTime
          RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"""

    val topProducts = spark.sql(
      s"""SELECT DISTINCT
          product,
          max($eventTime) $productWindow - min($eventTime) $productWindow AS durationInSeconds
        FROM events
        ORDER BY durationInSeconds DESC
        LIMIT $limit""")
      .cache()

    topProducts.write
      .option("header", "true")
      .csv(outputPath)

    topProducts.show()
  }

  run()

}
