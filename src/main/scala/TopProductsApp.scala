/** Spark application to calculate top products by session duration.
  * Definition of a session: <em>session lasts until the user is looking at particular product.
  * When particular user switches to another product the new session starts.</em>
  * Input example could be found at `data/example.csv`.
  *
  * Input format: `category,product,userId,eventTime,eventType`
  *
  * Output format: `category,product,sumDurationInSeconds,productRank`
  */
object TopProductsApp extends GenericApp {

  def appName = "top-products-app"

  def execute(inputPath: String, outputPath: String) = withSpark { spark =>
    val events = spark.read
      .option("header", "true")
      .schema(Event.schema)
      .csv(inputPath)

    events.createTempView("events")
    spark.udf.register("session_id", new ContinuousSessionId)

    val eventsWithSessions = spark.sql("""SELECT
      userId, category, product, eventTime,
      session_id(product) OVER (PARTITION BY userId ORDER BY eventTime) AS sessionId
      FROM events""")

    eventsWithSessions.createTempView("eventsWithSessions")

    val sessions = spark.sql("""SELECT userId, category, product, sessionId,
      unix_timestamp(max(eventTime)) - unix_timestamp(min(eventTime)) as duration
      FROM eventsWithSessions GROUP BY userId, category, product, sessionId""")

    sessions.createTempView("sessions")

    val productDurations = spark.sql("""SELECT category, product,
      sum(duration) as sumDurationInSeconds
      FROM sessions GROUP BY category, product""")

    productDurations.createTempView("productDurations")

    val productRanks = spark.sql("""SELECT category, product, sumDurationInSeconds,
      row_number() OVER (PARTITION BY category ORDER BY sumDurationInSeconds DESC) AS productRank
      FROM productDurations""")

    productRanks.createTempView("productRanks")

    val topProducts = spark.sql("""SELECT
      category, product, sumDurationInSeconds, productRank
      FROM productRanks WHERE productRank <= 10""").cache()

    topProducts.write
      .option("header", "true")
      .csv(outputPath)

    topProducts.show()
  }

  run()

}
