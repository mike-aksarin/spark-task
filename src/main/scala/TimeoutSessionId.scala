
import java.sql.Timestamp
import java.util.UUID
import java.util.logging.Logger

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructType, TimestampType}

/** User defined aggregate function to generate session id.
  * The session is assumed to be a period when next events comes
  * after the previous event not later than after a timeout given.
  * Works properly for `UNBOUNDED PRECEDING AND CURRENT ROW` window only.
  * The function itself take one parameter:
  *
  * - `eventTime: Timestamp` The event time field to determine the session boundaries.
  *
  * Function class constructor takes a `timeout` parameter to specify
  * maximum user inactivity period.
  * @param timeout session inactivity timeout in seconds
  */
class TimeoutSessionId(timeout: Long) extends UserDefinedAggregateFunction {

  def log = Logger.getLogger("SESSION_ID")

  override val inputSchema: StructType = new StructType()
    .add("eventTime", TimestampType)

  val eventTimeIdx = 0

  override val bufferSchema: StructType = new StructType()
    .add("sessionId", StringType)
    .add("previousTime", TimestampType)

  val (sessionIdx, prevTimeIdx) = (0, 1)

  override def dataType: DataType = StringType

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val sessionId = nextId()
    buffer.update(sessionIdx, sessionId)
    log.info(s"INITIALIZE: $sessionId")
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val eventTime = input.getTimestamp(eventTimeIdx)
    if (!buffer.isNullAt(prevTimeIdx)) {
      val prevTime = buffer.getTimestamp(prevTimeIdx)
      log.info(s"UPDATE by $eventTime, prevTime is $prevTime, prevSession is ${buffer.getString(sessionIdx)}")
      if (!sameSession(prevTime, eventTime)) {
        val sessionId = nextId()
        buffer.update(sessionIdx, sessionId)
        log.info(s"NEW SESSION by $eventTime, prevTime is $prevTime, $sessionId")
      }
    }
    buffer.update(prevTimeIdx, eventTime)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    throw new Error(s"Merge should not be called! " +
                    s"${getClass.getSimpleName} should be used only as a window function")
  }

  /** No need to call `evaluate` multiple times if no update occurred */
  override def deterministic: Boolean = true

  override def evaluate(buffer: Row): String = {
    val sessionId = buffer.getString(sessionIdx)
    log.info(s"EVALUATE: $sessionId")
    sessionId
  }

  def sameSession(prevTime: Timestamp, curTime: Timestamp): Boolean = {
    Math.abs(curTime.getTime - prevTime.getTime) <= timeout * 1000
  }

  def nextId(): String = UUID.randomUUID().toString

}
