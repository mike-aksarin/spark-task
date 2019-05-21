import java.sql.Timestamp

import org.apache.spark.sql.{Encoder, Encoders}

/** Event data type used by `SessionAggregateApp` to evaluate the input
  */
case class Event(
  category: String,
  product: String,
  userId: String,
  eventTime: Timestamp,
  eventType: String
)

object Event {
  implicit val encoder: Encoder[Event] = Encoders.product[Event]
  val schema = encoder.schema

  val Array(category, product, userId, eventTime, eventType) = schema.fieldNames
}