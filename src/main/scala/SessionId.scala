
import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructType, TimestampType}

/** User defined aggregate function to generate session id.
  * The session is assumed to be a period when next events comes
  * after the previous event not later than after a timeout given.
  * The function itself take one parameter:
  *
  * - `eventTime: Timestamp` The event time field to determine the session boundaries.
  *
  * Function class constructor takes a `timeout` parameter to specify
  * maximum user inactivity period.
  * @param timeout session inactivity timeout in seconds
  */
class SessionId(timeout: Long) extends UserDefinedAggregateFunction {

  override val inputSchema: StructType = new StructType()
    .add("eventTime", TimestampType)

  val eventTimeIdx = 0

  override val bufferSchema: StructType = new StructType()
    .add("sessionId", StringType)
    .add("previousTime", TimestampType)

  val (sessionIdx, prevTimeIdx) = (0, 1)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = false

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(sessionIdx, nextId())
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val eventTime = input.getAs[Timestamp](eventTimeIdx)
    if (!buffer.isNullAt(prevTimeIdx)) {
      val prevTime = buffer.getAs[Timestamp](prevTimeIdx)
      if (!sameSession(prevTime, eventTime)) {
        buffer.update(sessionIdx, nextId())
      }
    }
    buffer.update(prevTimeIdx, eventTime)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (!buffer2.isNullAt(prevTimeIdx)) {
      val buffer2Time = buffer2.getAs[Timestamp](prevTimeIdx)
      if (!buffer1.isNullAt(prevTimeIdx)) {
        val buffer1Time = buffer1.getAs[Timestamp](prevTimeIdx)
        if (!sameSession(buffer1Time, buffer2Time)) {
          buffer1.update(sessionIdx, buffer2.getAs[String](sessionIdx))
        }
      }
      buffer1.update(prevTimeIdx, buffer2Time)
    }
  }

  override def evaluate(buffer: Row): String = buffer.getAs[String](sessionIdx)

  def sameSession(prevTime: Timestamp, curTime: Timestamp): Boolean = {
    Math.abs(curTime.getTime - prevTime.getTime) <= timeout * 1000
  }

  def nextId(): String = UUID.randomUUID().toString

}
