import SparkBigData._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

object UseCaseBANO {
  // Creation du schema du dataset BANO

  val schema_bano = StructType(Array(
    StructField("id_bano", StringType, false),
    StructField("numero_voie", StringType, false),
    StructField("nom_voie", StringType, false),
    StructField("code_postal", StringType, false),
    StructField("nom_commune", IntegerType, false),
    StructField("code_source_bano", StringType, false),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true)
  )
  )
// Configuration Hadoop file system
   val configHadoop = new Configuration()
   val fs = FileSystem.get(configHadoop)

  def main(args: Array[String]): Unit = {

    val ss = Session_spark(true)

    val df_bano_brut = ss.read
      .format("com.databricks.spark.csv")
     // .schema(schema_bano)
      //.option("inferSchema",true)
      .load("C:\\fichier_bano\\full.csv")

 val df_bano = df_bano_brut.withColumnRenamed("_c0","id_bano")
   .withColumnRenamed("_c1","numero_voie")
   .withColumnRenamed("_c2" , "nom_voie")
   .withColumnRenamed("_c3" , "code_postal")
   .withColumnRenamed("_c4","nom_commune")
   .withColumnRenamed("_c5","code_source_bano")
   .withColumnRenamed("_c6","latitude")
   .withColumnRenamed("_c7","longitude")

    df_bano.show(25)




  }

}
