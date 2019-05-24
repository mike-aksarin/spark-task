import java.io.{File, PrintWriter}
import java.sql.Timestamp

import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}

import scala.util.Random

 /**  This is a simple app to ensure that [[TimeoutSessionId]] user-defined window function works properly for `UNBOUNDED PRECEDING AND CURRENT ROW` window.
   <pre>
    == Physical Plan ==
    InMemoryTableScan [rowId#0L, userId#1, eventTime#2, sessionId#6]
       +- InMemoryRelation [rowId#0L, userId#1, eventTime#2, sessionId#6], StorageLevel(disk, memory, deserialized, 1 replicas)
         +- Window [timeoutsessionid(eventTime#2, TimeoutSessionId@72585e83, 0, 0) windowspecdefinition(userId#1, eventTime#2 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS sessionId#6], [userId#1], [eventTime#2 ASC NULLS FIRST]
            +- *(2) Sort [userId#1 ASC NULLS FIRST, eventTime#2 ASC NULLS FIRST], false, 0
               +- Exchange hashpartitioning(userId#1, 200)
                  +- *(1) FileScan csv [rowId#0L,userId#1,eventTime#2] Batched: false, Format: CSV, Location: InMemoryFileIndex[hdfs://localhost:9000/data/simple.100m.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<rowId:bigint,userId:string,eventTime:timestamp>
   </pre>
  */
object SimpleSessionApp extends GenericApp {

  def appName = "session-id"

  val users = Array("Gandalf", "Bilbo", "Samwise", "Frodo", "Aragorn",
                    "Legolas", "Gimli", "Peregrin", "Meriadoc", "Faramir",
                    "Arwen", "Treebeard", "Bombadil", "Eomer", "Eowyn")

  def timeout = users.length * 60 // = 15 min (for 15 users)

  def execute(inputPath: String, outputPath: String): Unit = withSpark { spark =>
    val schema = new StructType()
      .add("rowId", LongType)
      .add("userId", StringType)
      .add("eventTime", TimestampType)

    val events = spark.read.schema(schema).csv(inputPath)

    events.createTempView("events")
    spark.udf.register("session_id", new SimpleSessionId)

    val sessions = spark.sql("""SELECT *,
     session_id(eventTime) OVER (PARTITION BY userId ORDER BY eventTime
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sessionId
     FROM events""").cache() // ORDER BY rowId

    sessions.explain(true)
    sessions.write.option("header", "true").csv(outputPath)

    val count = sessions.select("sessionId").distinct().count()
    sessions.show(100)
    println(s"Number of sessions: $count")
  }

  def generateRows(outputFilePath: String = "simple.100m.csv", numberOfRows: Int = 1 << 27) = {
    val startTime: Long = System.currentTimeMillis() / 3600000 * 3600000
    val writer = new PrintWriter(new File(outputFilePath))
    try {
      (1 to numberOfRows).foreach { i =>
        val user = users(Random.nextInt(users.length))
        val eventTime = new Timestamp(startTime + i * 60000L)
        writer.println(s"$i, $user, $eventTime")
      }
    } finally writer.close()
  }

  run()

}