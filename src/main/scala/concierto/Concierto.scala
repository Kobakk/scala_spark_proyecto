package concierto

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger

object Concierto{
  private val logger = Logger.getLogger(getClass.getName)
  def procesar(spark: SparkSession): Unit = {
    // 1. Leemos de forma flexible (igual que en Python)
    val df = spark.read
      .option("header", "true")     // Usa la primera línea como nombres de columna
      .option("inferSchema", "true") // Detecta si es número o texto solo
      .option("quote", "\"")        // IMPORTANTE: Para que Taylor Swift no se rompa con las comas de los precios
      .option("escape", "\"")
      .csv("data/concierto.csv")

    // 2. Mostramos tal cual
    println("Mostrando datos originales del CSV:")
    df.show(5, truncate = false) // truncate = false para ver los nombres largos de los tours

    // 3. Si quieres ver qué columnas ha detectado
    df.printSchema()
  }
}