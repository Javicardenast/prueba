package sample

import org.apache.commons.cli.{BasicParser, Options}

case class ConfigArgs() extends Serializable {

  //PANEL DE AYUDA
  private val OPT_HELP = ("h","help")
  private val OPT_FECHA_TMESTAMP = ("ts","timeStamp")

  lazy private val OPT_FECHA = ("p_date","fecha")
  lazy private val OPT_TIP_EXE = ("p_date","fecha")
  lazy private val OPT_BBDD = ("p_date","fecha")
  lazy private val OPT_BDD_BDR = ("p_date","fecha")
  lazy private val OPT_BBDD_PG = ("p_date","fecha")
  lazy private val OPT_ENTIDAD = ("p_date","fecha")
  lazy private val OPT_FLAG_ORIGEN = ("p_date","fecha")
  lazy private val OPT_ENTORNO = ("p_date","fecha")
  lazy private val OPT_TABLA_AGG = ("p_agg","tablaAgg")
  lazy private val OPT_TABLA_DESC = ("p_desc","tablaDesc")
  lazy private val OPT_TABLA_CLASIF =("p_clasif","tablaClasif")
  lazy private val OPT_TABLA_GARA =("p_gara","tablaGara")
  lazy private val OPT_TABLA_REEST = ("p_reest","tablaReest")
  lazy private val OPT_TABLA_PAGOS = ("p_pagos","tablaPagos")
  lazy private val OPT_TABLA_VCART = ("p_vcart","tablaVcart")

  //Valores x defecto
  private var help = false
  private var outFecha: String = null
  private var tipExe: String = null
  private var idPG: String = null
  private var urlPG: String = null
  private var bdPG: String = null

  //Valores x defecto de configuración
  private var outFechaTimeStamp: String = null
  private var bbdd: String =null
  private var entidad: String =null
  private var flagOrigen: String =null
  private var entorno: String =null

  //Valores x defecto de tablas origen
  private var tablaAgg: String =null
  private var tablaDesc: String =null
  private var tablaClasif: String =null
  private var tablaGara: String =null
  private var tablaReest: String =null
  private var tablaPagos: String =null
  private var tablaVcart: String =null

  def getBBDD = bbdd

  def getOutFecha: String = outFecha

  def gparse(args: Array[String]) = {
    val parser = new BasicParser()
    val cmd = parser.parse(Options,args)
  }
}
