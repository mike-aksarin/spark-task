
import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructType, TimestampType}

/** User defined aggregate function to find out the session start time.
  * The session is assumed to be a period with the same value of `partition` field.
  * Takes two parameters:
  *
  * - `partition: String` The partition field to determine session boundaries.
  *
  * - `eventTime: Timestamp` The event time field to determine start time for the new session.
  */
class SessionStartTime extends UserDefinedAggregateFunction {

  override val inputSchema: StructType = new StructType()
    .add("partition", StringType)
    .add("eventTime", TimestampType)

  val (partIdx, eventTimeIdx) = (0, 1)

  override val bufferSchema: StructType = new StructType()
    .add("previousPartition", StringType)
    .add("previousStartTime", TimestampType)

  val (prevPartIdx, prevStartTimeIdx) = (0, 1)

  override def dataType: DataType = TimestampType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = ()

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val partition = input.getAs[String](partIdx)
    val eventTime = input.getAs[Timestamp](eventTimeIdx)
    if (buffer.isNullAt(prevPartIdx) || buffer.getAs[String](prevPartIdx) != partition) {
      buffer.update(prevStartTimeIdx, eventTime)
      buffer.update(prevPartIdx, partition)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (!buffer2.isNullAt(prevPartIdx)) {
      val buffer2Part = buffer2.getAs[String](prevPartIdx)
      if (buffer1.isNullAt(prevPartIdx) || buffer1.getAs[String](prevPartIdx) != buffer2Part) {
        buffer1.update(prevStartTimeIdx, buffer2.getAs[Timestamp](eventTimeIdx))
        buffer1.update(prevPartIdx, buffer2Part)
      }
    }
  }

  override def evaluate(buffer: Row): Timestamp = buffer.getAs[Timestamp](prevStartTimeIdx)

}
