package sample

import org.antlr.v4.runtime.misc.LogManager
import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.io.File
import scala.reflect.macros.ParseException
//import spark.implicits._

object main {

  //Spark log colores
  val LOG = LogManager.getLogger(getClass.getName)
  LOG.setLevel(Level.INFO)
  val CONSOLE_MAGENTA = ""

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val spark = SparkSession.builder()
    .appName("main")
    .config("spark.master",local)
    .config("spark.hadoop.fs.defaultFS","hdfs://localhost:9000")
    .getOrCreate()

  val jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/prueba_sql")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "prueba_sql")
    .option("user", "lct638")
    .option("password", "Almeria&99")
    .load()

  jdbcDF.createOrReplaceTempView("mytable")

  val sqlDF = spark.sql("SELECT * FROM mytable")
  sqlDF.show()

  /*
  val schema = StructType(List(
    StructField("ID",IntegerType,true),
    StructField("contrato",StringType,true),
    StructField("MENSAJE_HUMANO",StringType,true)))

  val logERROR = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val cfg = new ConfigArgs()
    try {
      cfg.parse(args)

      if (cfg.mostrarAyuda) {
        cfg.printHelp()
        System.exit(0)
      }
    } catch {
      case e: ParseException => LOG.error(CONSOLE_MAGENTA + "ERROR" + e.getMessage + Console.RESET)
        cfg.printHelp()
        System.exit(0)
    }
    try {
      println("### APLICATION ID: " + spark.sparkContext.applicationId)
      val bbdd = cfg.getBBDD
      val odate = cfg.getOutFecha
      val odateFormat = odate.substring(0, 4) + "-" + odate.substring(4, 6) + "-" + odate.substring(6, 8)

      println("Lecturas de tablas")

      val pruebaLectura = spark.sql("select * from" + bbdd + ".prueba")

      println("PARAMETROS: OK")
      println("FECHA: " + odateFormat)
      println("BBDD: " + bbdd)
      println("LECTURA TABLA " + pruebaLectura)

*/
    }
  }
}
