package sample

import org.antlr.v4.runtime.misc.LogManager
import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql._
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.io.File
import scala.reflect.macros.ParseException
import spark.implicits._

object main {

  //Spark log colores
  val LOG = LogManager.getLogger(getClass.getName)
  LOG.setLevel(Level.INFO)
  val CONSOLE_MAGENTA = ""

  val spark = SparkSession.builder()
    .appName("main")
    .config("spark.master", local)
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .getOrCreate()


  val jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/prueba")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "prueba_sql")
    .option("user", "lct638")
    .option("password", "script")
    .load()

  jdbcDF.createOrReplaceTempView("mytable")

  val sqlDF = spark.sql("SELECT * FROM mytable")
  sqlDF.show()

  def aplicacionDeficit: Unit ={
    import spark.implicits._

    /*

    SHORTFALL -- Se refiere a la cantidad de dinero que falta para cubrir una obligación financiera.
    Para el deficit hay que realizar una serie de metricas, tanto las genéricas que afectan a todas las plantillas
    como las mas específicas.
      - Reglamento 575/2013 exposiciones garantizadas
          - exposiciones secured y unsecured

     En esta parte se agregará el shotfall de la unidad considerando todos los contratos que esten dentro del ámbito de la plantilla que se reporta,
     estos pueden ser: stock/addemdum/c35
     Tal desglose se irá realizando a través del ant_deudor...Es conocidido como antiguedad de la mora, y se refiere a la cantidad de tiempo
     que una obligación financiera ha estado en mora, es decir, que no se han pagado según los términos acordados.
     */

    
    val t_deudor = spark.sql("select * from datos.lista_deudores")
    val t_avalados= spark.sql("select * from datos.lista_avalados")


    //garantia_a (capital base)
    /* ---Explicación del proceso que se ejecuta aqui:
     *  Se calculan las metricas necesarias a nivel identidad.
     *  Coneste cruce conseguimos los contratos garantizados y sus respectivos filtros para los contratos.
     */
    val deudor_deficit = t_deudor
      .filter(t_deudor.col("flg_prueba") === "1")
      .select("identidad", "fdatos", "empresa", "estandar_riesgo_credito", "f_apertura","f_predeterminada","computo_dentro","computo_fuera","provision_total","popular")

    val avalados_deficit = t_deudor.join(t_avalados,Seq("identidad","fdatos","empresa"))
      .filter(t_deudor.col("flg_prueba") === "1")
      .select("identidad", "fdatos", "empresa", "estandar_riesgo_credito", "f_apertura","f_predeterminada","computo_dentro","computo_fuera","provision_total","popular")

    val deu_ava_deficit = deudor_deficit.join(avalados_deficit,Seq("identidad", "fdatos", "empresa", "estandar_riesgo_credito", "f_apertura", "f_predeterminada", "computo_dentro", "computo_fuera", "provision_total", "popular"))

    //Por consiguiente toca el calculo de exposiciones secured y unsecured.

    /*
    Se realizan el calculo de las exposiciones secured y unsecured, de esta forma primero hacemos una exposición total entre el sado que hay dentro y fuera.
      En la exposición secured: Está cubierta por la garantia
      En la exposición unsecred: No está cubierta por la garantia.

      Por consiguiente, anulamos null's y agrupamos en identidades para sacar una exposición secured total.
     */
    val cBase_exp = deu_ava_deficit
      .withColumn("expo_total",$"computo_dentro" + $"computo_fuera") //expo_total es la suma de computo dentro y fuera.

    val cBase_ava = deu_ava_deficit
      .withColumn("comp_asegurado",when($"asegurado").isNull, lit(0)).otherwise(col("asegurado"))
      .groupBy("identidad")
      .agg(sum($"exp_secured").as("exp_secured_total"))

    val exp_ava = cBase_exp.join(cBase_ava, Seq("identidad"))
      .drop($"asegurado")

    val cBase_exp_total = exp_ava
      .withColumn("exp_secured",when($"expo_total" <= $"exp_secured_total",$"exp_secured")
        .when($"expo_total" > $"exp_secured_total",$"exp_secured_total"))

      .withColumn("exp_unsecured", when($"expo_total" <= $"exp_secured_total", lit(0))
        .when($"expo_total" > $"exp_secured_total", $"exp_secured" - $"exp_secured_total"))


    //Calculo de cobertura mínima, factor secured y factor unsecured.
    val t_parametrizada = spark.sql("select * from datos.t_parametrizada")
    val ant_deudor = cBase_exp_total
      .withColumn("ant_deudor",(-datediff(t_deudor.col("f_predeterminada"),col("fdatos"))) / 365)

    val cBase_param = ant_deudor.join(t_parametrizada, Seq("fdatos"),"full_outer")
      .withColumn("deudor_1", when(col("ant_deudor") <= 1, lit(0))
      .when(whenRange("ant_deudor", 1, 2), lit(1))
      .when(whenRange("ant_deudor", 1, 2), lit(1))
      .when(whenRange("ant_deudor", 2, 3), lit(2))
      .when(whenRange("ant_deudor", 3, 4), lit(3))
      .when(whenRange("ant_deudor", 4, 5), lit(4))
      .when(whenRange("ant_deudor", 5, 6), lit(5))
      .when(whenRange("ant_deudor", 6, 7), lit(6))
      .when($"ant_deudor" > lit(7), lit(7)))

      .withColumn("deudor_2", when(col("ant_deudor") <= 1, lit(0))
      .when(whenRange("ant_deudor", 1, 2), lit(1))
      .when(whenRange("ant_deudor", 1, 2), lit(1))
      .when(whenRange("ant_deudor", 2, 3), lit(2))
      .when(whenRange("ant_deudor", 3, 4), lit(3))
      .when(whenRange("ant_deudor", 4, 5), lit(4))
      .when(whenRange("ant_deudor", 5, 6), lit(5))
      .when(whenRange("ant_deudor", 6, 7), lit(6))
      .when($"ant_deudor" > lit(7), lit(7)))

       //Calculo de provisiones capped y uncapped
    /*
    Las provisiciones capped establece un porcentaje máximo de provisiones que se pueden reservar para cubrir pérdidas
    Las provisiciones uncapped no tienen un límite superior establecido y se calculan en función de los riesgos identificados en los préstamos.
     */
    val prov_capped = cBase_param
      .withColumn("prov_capped", when($"popular" === lit(1) || $"deudor1" <= $"provision_total", $"deudor1").otherwise($"provision_total"))

    val cBase_prov_secured = prov_capped
      .withColumn("provtotal_secured", $"provision_total" - $"exp_secured_total")
      .withColumn("prov_unsec",when($"provtotal_secured") < lit(0), lit(0).otherwise($"provtotal_secured"))
      .withColumn("prov_sec",when($"prov_unsec") === lit(0), $"provision_total".otherwise($"exp_secured_total"))

    //Calculo shortfall
    val cBase_deficit = cBase_prov_secured
      .withColumn("max_deficit",$"deudor1" - $"prov_capped")
      .withColumn("deficit_total",when($"max_deficit" > lit(0), $"max_deficit").otherwise(lit(0)))

    val output_cBase = cBase_deficit
      .filter($"estandar_riesgo_credito" === 1)
      .select("empresa","identidad","fdatos","ant_deudor","popular","deficit_total","deudor1","deudor2","exp_secured","exp_unsecured","provision_total","prov_sec","prov_unsec")


  }
}
