package concierto

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Pokemon{
  private val logger = Logger.getLogger(getClass.getName)

  def procesar(spark: SparkSession): Unit = {

    println("----------------- Inicio Proceso Pokemón -----------------")

// 1. DEFINICIÓN DE SCHEMAS EXPLÍCITOS
    val schemaTCG = StructType(Seq(
      StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("hp", IntegerType, true),
      StructField("types", StringType, true), // Columna multivalor (JSON/Array)
      StructField("rarity", StringType, true)
    ))

    val schemaUpdated = StructType(Seq(
      StructField("pokedex_number", IntegerType, true),
      StructField("name", StringType, true),
      StructField("type_1", StringType, true),
      StructField("type_2", StringType, true),
      StructField("ability_list", StringType, true) // Columna multivalor
    ))

    // 2. LECTURA DE ARCHIVOS
    val dfTCG = spark.read.option("header", "true").schema(schemaTCG).csv("data/pokemon_tcg.csv")
    val dfUpdated = spark.read.option("header", "true").schema(schemaUpdated).csv("data/pokemon_updated.csv")

    // 3. CONTEO DE FILAS Y COLUMNAS
    reportarDimensiones("TCG", dfTCG)
    reportarDimensiones("Updated", dfUpdated)

    // 4. DETECCIÓN DE NULOS
    println("\n--- Columnas con Nulos (Updated) ---")
    detectarNulos(dfUpdated)

    // 5. NORMALIZACIÓN Y CLAVES NATURALES
    // Normalizamos nombres: minúsculas, sin espacios, sin símbolos
    val dfUpdatedLimpio = dfUpdated
      .withColumn("name_norm", lower(regexp_replace(col("name"), "[^a-zA-Z0-9]", "")))
      .dropDuplicates("name_norm") // Identificar duplicados por clave natural (nombre normalizado)

    println(s"Duplicados eliminados. Filas tras limpieza: ${dfUpdatedLimpio.count()}")

    // 6. IDENTIFICACIÓN MULTIVALOR
    // Ejemplo: Si la columna parece un JSON o una lista separada por comas
    val dfFinal = dfUpdatedLimpio
      .withColumn("is_multivalue", col("ability_list").contains(",") || col("ability_list").contains("["))

    dfFinal.select("name", "ability_list", "is_multivalue").show(5)

    println("----------------- Final Proceso Pokémon -----------------")
  }

  // --- FUNCIONES DE APOYO ---

  def reportarDimensiones(nombre: String, df: DataFrame): Unit = {
    println(s"Dataset $nombre -> Filas: ${df.count()}, Columnas: ${df.columns.length}")
  }

  def detectarNulos(df: DataFrame): Unit = {
    df.select(df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show()
  }

}
