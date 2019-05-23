
import java.util.UUID
import java.util.logging.Logger

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructType}

/** User defined aggregate function to find out the session start time.
  * The session is assumed to be a period with the same value of `partition` field.
  * Works properly for `UNBOUNDED PRECEDING AND CURRENT ROW` window only.
  * Takes single parameter:
  *
  * - `partition: String` The partition field to determine session boundaries.
  */
class ContinuousSessionId extends UserDefinedAggregateFunction {

  def log = Logger.getLogger("SESSION_ID")

  override val inputSchema: StructType = new StructType()
    .add("partition", StringType)

  val partIdx = 0

  override val bufferSchema: StructType = new StructType()
    .add("sessionIdx", StringType)
    .add("previousPartition", StringType)

  val (sessionIdx, prevPartIdx) = (0, 1)

  override def dataType: DataType = StringType

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    log.info(s"INITIALIZE")
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val partition = input.getString(partIdx)
    log.info(s"UPDATE by $partition, prevPart is ${buffer.getString(prevPartIdx)}," +
             s" prevSession is ${buffer.getString(sessionIdx)}")
    if (buffer.isNullAt(prevPartIdx) || buffer.getString(prevPartIdx) != partition) {
      val sessionId = nextId()
      log.info(s"NEW SESSION $sessionId by $partition, " +
               s"prevPart is ${buffer.getString(prevPartIdx)}")
      buffer.update(prevPartIdx, partition)
      buffer.update(sessionIdx, sessionId)
    }
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

  def nextId(): String = UUID.randomUUID().toString

}
