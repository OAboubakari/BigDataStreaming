import org.apache.spark.sql._
import org.apache.spark.sql.types
import org.apache.spark.sql.functions._
import SparkBigData._
import org.elasticsearch.spark.sql._
import java.util._


object Spark_ElasticSearch {

  def main(args: Array[String]): Unit = {
    // Lecture du fichier csv à indexer
    val session_elasticsearch = Session_spark(true)
    val df_index = session_elasticsearch.read
      .format("com.databricks.spark.csv")
      .option("delimiter" , ";")
      .option("header" ,"true")
      .load("C:\\Users\\PC\\Desktop\\Maîtrisez Spark pour le Big Data avec Scala\\sources de données\\orders.csv")


    //df_index.show(15)

    //Définition des paramètres de connexion avec le cluster Elastic search et ecriture dans elasticsearch

    df_index.write
      .mode(SaveMode.Overwrite)
      .format("org.elasticsearch.spark.sql")
      .option("es.port","9200")
      .option("es.nodes" , "localhost:9200")
      .option("es.net.http.auth.user","elastic")
      .option("es.nodes.client.transport.sniff","true")
      .option("es.net.http.auth.pass","cWNfEZr8N0u6SHoGS4Ls")
      .save("index_oa/doc")






  }

}
