
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Spark application to enrich events with session fields. Implemented with window aggregation functions.
  * Definition of a session: <em>for each user, it contains consecutive events
  * that belong to a single category and are not more than 5 minutes away from each other.</em>
  * Input example could be found at `data/example.csv`.
  *
  * Input format: `category,product,userId,eventTime,eventType`
  *
  * Output format: `category,product,userId,eventTime,eventType,sessionStartTime,sessionEndTime,sessionId`
  */
object SessionAggregateApp extends GenericApp {

  def appName = "session-aggregate-app"

  def execute(inputPath: String, outputPath: String) = withSpark { spark =>
    val events = spark.read
      .option("header", "true")
      .schema(Event.schema)
      .csv(inputPath)

    val timeWindow = Window
      .partitionBy(Event.userId, Event.category)
      .orderBy(Event.eventTime)

    val sessionWindow = Window.partitionBy("sessionId")

    val sessionId = new SessionId

    val sessions = events
      .withColumn("sessionId", sessionId(col(Event.eventTime)).over(timeWindow))
      .withColumn("sessionStartTime", min(Event.eventTime).over(sessionWindow))
      .withColumn("sessionEndTime", max(Event.eventTime).over(sessionWindow))
      .orderBy(col(Event.eventTime))
      .cache()

    sessions.write
      .option("header", "true")
      .csv(outputPath)

    sessions.show(30)
  }

  run()

}
