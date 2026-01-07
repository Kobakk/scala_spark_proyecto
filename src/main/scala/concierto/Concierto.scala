package concierto

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger

object Concierto{
  private val logger = Logger.getLogger(getClass.getName)
  def procesar(spark: SparkSession): Unit = {
    val path = "data/concierto.csv"
    logger.info(s"Modulo Conciertos: Leyendo archivo desde $path")

    // Definimos el esquema para ser explícitos y evitar errores de inferencia
    val esquemaConcierto = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("artista", StringType, true),
      StructField("ciudad", StringType, true),
      StructField("precio", DoubleType, true)
    ))

    // Lectura del CSV
    val dfConciertos = spark.read
      .option("header", "true")
      .schema(esquemaConcierto)
      .csv(path)

    logger.info("Datos de conciertos cargados correctamente:")
    dfConciertos.show()

    // Ejemplo de una pequeña transformación (conciertos caros)
    val conciertosCaros = dfConciertos.filter("precio > 50")

    logger.info("Conciertos con precio mayor a 50€:")
    conciertosCaros.show()
  }
}