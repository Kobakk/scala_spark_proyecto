import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import scala.util.{Try, Success, Failure}

object Main {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
    def main(args: Array[String]): Unit = {
        // 1. Configuración de la sesión con gestión de errores
        val spark = SparkSession.builder()
          .appName("SparkProcesamientoSeguro")
          .master("local[*]")
          .getOrCreate()

        // Configuramos el nivel de log para no saturar la consola
        Logger.getLogger("org").setLevel(Level.WARN) 
        logger.info("Iniciando aplicación Spark...")

        val resultado = Try {
          //ejecutarTransformaciones(spark)
          concierto.Concierto.procesar(spark)
        }

        resultado match {
          case Success(_) => 
            logger.info("Proceso completado con éxito.")
          case Failure(e) => 
            logger.error(s"Error crítico en el pipeline: ${e.getMessage}")
            e.printStackTrace()
        }

        spark.stop()
      }
    def ejecutarTransformaciones(spark: SparkSession): Unit = {
        val path = "dataset_windowing.txt"
        logger.info(s"Leyendo archivo desde: $path")
        
        // 1. Leemos como RDD de texto (esto no necesita Encoders ni Implicits)
        val lineasRdd = spark.sparkContext.textFile(path)
        
        val header = lineasRdd.first()

        // 2. Procesamos el RDD para obtener filas (Row)
        val filasRdd = lineasRdd
          .filter(line => line != header && line.trim.nonEmpty)
          .map(line => {
            val cols = line.split(",")
            org.apache.spark.sql.Row(cols(0), cols(1), cols(2).trim.toDouble)
          })

        // 3. Definimos el esquema manualmente
        val schema = org.apache.spark.sql.types.StructType(Array(
          org.apache.spark.sql.types.StructField("nombre", org.apache.spark.sql.types.StringType, true),
          org.apache.spark.sql.types.StructField("asignatura", org.apache.spark.sql.types.StringType, true),
          org.apache.spark.sql.types.StructField("nota", org.apache.spark.sql.types.DoubleType, true)
        ))

        // 4. Creamos el DataFrame (Aquí es donde unimos los datos y la estructura)
        val df = spark.createDataFrame(filasRdd, schema)

        // 5. El resto de la lógica de ventana (Esto ya funciona bien)
        val windowSpec = Window.partitionBy("asignatura").orderBy(desc("nota"))

        val dfFinal = df
          .withColumn("ranking", dense_rank().over(windowSpec))
          .withColumn("tier", when(col("ranking") <= 3, "Tier 1")
            .when(col("ranking") <= 10, "Tier 2")
            .otherwise("Tier 3"))
          .groupBy("asignatura", "tier")
          .agg(mean("nota").as("promedio_nota"))

        dfFinal.show()
        
        logger.info("Guardando resultados en output/resultado_final...")
        dfFinal.write.mode("overwrite").parquet("output/resultado_final")
      }
    }
