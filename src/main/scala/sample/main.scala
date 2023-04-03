package sample

import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.io.File
//import spark.implicits._

object main {

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val spark = SparkSession.builder()
    .appName("main")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  val schema = StructType(List(
    StructField("ID",IntegerType,true),
    StructField("contrato",StringType,true),
    StructField("MENSAJE_HUMANO",StringType,true)))

  val logERROR = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)

  def main(args: Array[String]): Unit ={
    BasicConfigurator.configure()
    val cfg = new ConfigArgs()
    try{
      cfg.parse(args)
    }


  }
}
