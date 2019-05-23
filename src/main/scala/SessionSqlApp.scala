/** Spark application to enrich events with session fields. Implemented with sql window functions.
  * Definition of a session: <em>for each user, it contains consecutive events
  * that belong to a single category and are not more than 5 minutes away from each other.</em>
  * Input example could be found at `data/example.csv`.
  *
  * Input format: `category,product,userId,eventTime,eventType`
  *
  * Output format: `category,product,userId,eventTime,eventType,sessionStartTime,sessionEndTime,sessionId`
  */
object SessionSqlApp extends GenericApp {

  def appName = "session-sql-app"

  /** Session inactivity timeout in seconds */
  def timeout: Int = 5 * 60

  def execute(inputPath: String, outputPath: String) = withSpark { spark =>
    val events = spark.read
      .option("header", "true")
      .schema(Event.schema)
      .csv(inputPath)

    events.createTempView("events")
    spark.udf.register("session_id", new TimeoutSessionId(timeout))

    val sessions = spark.sql(s"""SELECT *,
      min(eventTime) OVER(PARTITION BY sessionId) AS sessionStartTime,
      max(eventTime) OVER(PARTITION BY sessionId) AS sessionEndTime
      FROM (SELECT *, session_id(eventTime) OVER (PARTITION BY userId, category
            ORDER BY eventTime) AS sessionId
            FROM events)
      ORDER BY eventTime""")
      .cache()

    sessions.write
      .option("header", "true")
      .csv(outputPath)

    sessions.show(30)
  }

  run()

}
