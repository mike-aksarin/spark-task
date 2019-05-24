
import java.util.UUID
import java.util.logging.Logger

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructType, TimestampType}

/** User defined aggregate function to generate session ID.
  * It just generates a new unique ID per each `initialize` call.
  * This helps to understand how window functions work for different types of windows
  * during experiments
  */
class SimpleSessionId extends UserDefinedAggregateFunction {

  def log = Logger.getLogger("SESSION_ID")
  val sessionIdx = 0
  val eventTimeIdx = 0

  override val inputSchema: StructType = new StructType().add("eventTime", TimestampType)
  override val bufferSchema: StructType = new StructType().add("sessionId", StringType)
  override def dataType: DataType = StringType

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val sessionId = nextId()
    buffer.update(sessionIdx, sessionId)
    log.info(s"INITIALIZE: $sessionId")
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    log.info(s"UPDATE ${buffer.getString(sessionIdx)} by ${input.getTimestamp(eventTimeIdx)}")
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    throw new Error(s"Merge should not be called! " +
                    s"${getClass.getSimpleName} should be used only as a window function")
  }

  override def deterministic: Boolean = false

  override def evaluate(buffer: Row): String = {
    val sessionId = buffer.getString(sessionIdx)
    log.info(s"EVALUATE: $sessionId")
    sessionId
  }

  def nextId(): String = UUID.randomUUID().toString

}
