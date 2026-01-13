package pokemon

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Pokemon{
  private val logger = Logger.getLogger(getClass.getName)

  def procesar(spark: SparkSession): Unit = {

    println("----------------- Inicio Proceso Pokemón -----------------")
    val a = 10;
    // 1. SCHEMAS EXPLÍCITOS (Basados en tus archivos)
    val schemaUpdated = StructType(Seq(
      StructField("#", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Type 1", StringType, true),
      StructField("Type 2", StringType, true),
      StructField("HP", IntegerType, true),
      StructField("Attack", IntegerType, true),
      StructField("Defense", IntegerType, true),
      StructField("Sp. Atk", IntegerType, true),
      StructField("Sp. Def", IntegerType, true),
      StructField("Speed", IntegerType, true),
      StructField("Generation", IntegerType, true),
      StructField("Legendary", StringType, true),
      StructField("Total", IntegerType, true)
    ))

    // 2. CARGA Y CONVERSIÓN
    val dfPokedexRaw = spark.read.option("header", "true").schema(schemaUpdated).csv("data/pokemon_updated.csv")
    val dfTCGRaw = spark.read.option("header", "true").option("inferSchema", "true").csv("data/pokemon_tcg.csv")

    // 3. NORMALIZACIÓN Y CLAVES NATURALES
    val cleanNameUDF = (c: String) => lower(regexp_replace(col(c), "[^a-zA-Z0-9]", ""))

    val dfPokedex = dfPokedexRaw
      .withColumn("name_norm", cleanNameUDF("Name"))
      .withColumn("is_legendary", col("Legendary") === "True")
      .dropDuplicates("name_norm") // Identificar duplicados y limpiar

    val dfTCG = dfTCGRaw
      .withColumn("name_norm", cleanNameUDF("name"))
      .withColumn("hp_numeric", col("hp").cast(IntegerType))

    // 4. ANÁLISIS DE NULOS Y MULTIVALOR
    println(s"Columnas Pokedex: ${dfPokedex.columns.length} | Filas: ${dfPokedex.count()}")
    println("Nulos en Pokedex:")
    dfPokedex.select(dfPokedex.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show()

    // 5. EXTRACCIÓN DE ATAQUES (JSON a Columnas)
    // Usamos regex para extraer el daño de la columna 'attacks' que es un string JSON
    val dfTCGWithAttacks = dfTCG
      .withColumn("attack_list", regexp_extract_all(col("attacks"), lit("'damage': '(\\d+)'"), 1))
      .withColumn("num_attacks", size(col("attack_list")))
      .withColumn("max_damage", aggregate(col("attack_list"), lit(0), (acc, x) => greatest(acc, x.cast("int"))))
      .withColumn("avg_damage", aggregate(col("attack_list"), lit(0.0), (acc, x) => acc + x.cast("double")) / col("num_attacks"))

    // 6. JOINS Y RELACIONES 1:N
    // Join para crear el Dataset Maestro
    val dfMaestro = dfPokedex.join(dfTCGWithAttacks, Seq("name_norm"), "inner").cache()

    // Analizar huérfanos
    val sinCartas = dfPokedex.join(dfTCG, Seq("name_norm"), "left_anti").count()
    val sinPokedex = dfTCG.join(dfPokedex, Seq("name_norm"), "left_anti").select("name").distinct().count()
    println(s"Pokémon sin cartas: $sinCartas | Cartas sin entrada en Pokédex: $sinPokedex")

    // 7. ANALÍTICA DE STATS (Pokédex)
    println("\n--- TOP STATS BASE ---")
    dfPokedex.orderBy(desc("Attack")).select("Name", "Attack").show(3)
    dfPokedex.orderBy(desc("Speed")).select("Name", "Speed").show(3)

    println("\n--- MEDIAS POR TIPO Y GENERACIÓN ---")
    dfPokedex.groupBy("Type 1").avg("Attack", "Defense").show(5)
    dfPokedex.groupBy("Generation").avg("Total").orderBy("Generation").show()

    // Pokémon más balanceado (Varianza mínima entre HP, Attack y Defense)
    val balanceCol = (pow(col("HP") - (col("Total")/13), 2) + pow(col("Attack") - (col("Total")/13), 2)).alias("varianza")
    dfPokedex.withColumn("varianza", balanceCol).orderBy("varianza").select("Name", "varianza").show(3)

    // 8. ANALÍTICA TCG
    println("\n--- ANALÍTICA TCG ---")
    // Pokémon con más cartas (Relación 1:N)
    dfMaestro.groupBy("Name").count().orderBy(desc("count")).show(5)

    // Cartas sin ataques o ataques sin daño
    val cartasSinDanio = dfTCGWithAttacks.filter(col("num_attacks") === 0 || col("max_damage") === 0).count()
    println(s"Cartas sin capacidad de daño: $cartasSinDanio")

    // Media de daño por rareza
    dfTCGWithAttacks.groupBy("rarity").agg(avg("max_damage").alias("danio_medio")).orderBy(desc("danio_medio")).show(5)

    // 9. ATAQUE MÁXIMO POR TIPO (Dataset Maestro)
    println("\n--- DAÑO MÁXIMO POR TIPO (TCG) ---")
    dfMaestro.groupBy("Type 1").agg(max("max_damage").alias("Max_Damage_TCG")).show(5)

    println("--- PROCESO FINALIZADO ---")
  }

  // Función auxiliar para extraer múltiples matches de regex (Spark 3.1+)
  def regexp_extract_all(c: org.apache.spark.sql.Column, exp: org.apache.spark.sql.Column, group: Int) = {
    expr(s"regexp_extract_all($c, $exp, $group)")
  }

}
