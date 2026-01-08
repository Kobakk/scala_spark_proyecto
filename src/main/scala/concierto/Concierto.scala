package concierto

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Concierto{
  private val logger = Logger.getLogger(getClass.getName)

  def procesar(spark: SparkSession): Unit = {

    println("----------------- Comienzo de Ejecucion -----------------")

    // --- PASO 1: LECTURA Y LIMPIEZA DE CABECERAS (Vital) ---
    val dfInicial = spark.read
      .option("header", "true")     // Usa la primera línea como nombres de columna
      .option("inferSchema", "true") // Detecta si es número o texto solo
      .option("quote", "\"")        // IMPORTANTE: Para que Taylor Swift no se rompa con las comas de los precios
      .option("escape", "\"")
      .csv("data/concierto.csv")
    val nombresCorrectos = Seq(
          "Rank", "Peak", "All Time Peak", "Actual gross", 
          "Adjusted gross (in 2022 dollars)", "Artist", "Tour title", 
          "Year(s)", "Shows", "Average gross", "Ref."
        )
    // Esto elimina los espacios invisibles de los nombres de las columnas
    val dfLimpio = dfInicial.toDF(nombresCorrectos: _*)

    // --- PASO 2: TRANSFORMACIONES DE TEXTO Y FECHAS ---
    val dfConFechas = dfLimpio
      .withColumn("Year", regexp_extract(col("Year(s)"), "(\\d{4})", 1))
      .withColumn("All Time Peak", regexp_extract(col("All Time Peak"), "(\\d+)", 1))
      .drop("Peak", "Year(s)")

    // --- PASO 3: TRANSFORMACIONES ECONÓMICAS (Cálculos) ---
    val dfEconomico = dfConFechas
      .withColumn("Actual gross_Num", regexp_replace(col("Actual gross"), "[^0-9.]", "").cast("double"))
      .withColumn("Adjusted gross_Num", regexp_replace(col("Adjusted gross (in 2022 dollars)"), "[^0-9.]", "").cast("double"))
      .withColumnRenamed("Adjusted gross (in 2022 dollars)", "Adjusted gross")
    // calculamos la diferencia y el sumatorio 
    val dfFinal = dfEconomico
      .withColumn("Diff gross", col("Adjusted gross_Num") - col("Actual gross_Num"))
      .withColumn("Total actual gross", sum(col("Actual gross_Num")).over(Window.partitionBy("Artist")))
    // --- PASO 4: CONSULTAS FINALES ---
    println("--- Ordenado por Rank Ascendente ---")
    dfFinal.orderBy(col("Rank").asc).show(10)

    println("--- Ordenado por Total Actual Gross Descendente ---")
    dfFinal.orderBy(col("Total actual gross").desc).show(10)

    println("----------------- Final de Ejecucion -----------------")
  }
}
