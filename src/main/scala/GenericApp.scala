import org.apache.spark.sql.SparkSession

/** Generic trait with helper methods for the spark apps.
  */
trait GenericApp extends App {

  /** An application name to configure spark context. To be overridden by concrete apps */
  def appName: String

  /** Main logic of the app to be overridden by concrete apps
    * @param inputPath input CSV files location
    * @param outputPath output CSV files location
    */
  def execute(inputPath: String, outputPath: String): Unit

  /** Parse application arguments and execute an application calling `execute` method.
    * In case of improper arguments show the usage string
    */
  def run(): Unit =  args match {
    case Array(input, output) =>
      execute(input, output)
    case _ =>
      println(usageString)
      System.exit(1)
  }

  /** The application usage string
    */
  def usageString: String = {
    s"Usage: spark-submit --class ${getClass.getName} <...> input-file-path output-dir-name"
  }

  /** Initialize the spark session and call a block over it
    */
  def withSpark(block: SparkSession => Unit): Unit = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()
    try block(spark) finally spark.close()
  }

}
