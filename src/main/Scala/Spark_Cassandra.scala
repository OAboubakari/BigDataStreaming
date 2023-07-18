import org.apache.spark.sql._
import org.apache.spark.sql.types
import org.apache.spark.sql.functions._
import SparkBigData._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._

object Spark_Cassandra {


  def main(args: Array[String]): Unit = {
    ss = Session_spark(true)
// creation du catalog cassandra

    ss.conf.set(s"spark.sql.catalog.sp_cassandra","com.datastax.spark.connector.datasource.CassandraCatalog")
    ss.conf.set(s"spark.sql.catalog.sp_cassandra.spark.cassandra.connection.host", "127.0.0.1")

// Chargement de la keyspace demo

   /** val df_cassandra = ss.read
      .format("com.datastax.spark.connector")
      .options(Map("keyspace" -> "demo","table"->"spacecraft_journey_catalog" , "cluster"->"journey_id"))
      .load()
    */

    val df_cassandra = ss.read
        .cassandraFormat(table="spacecraft_journey_catalog" , keyspace = "demo" , cluster ="journey_id" )
        .load()


    df_cassandra.printSchema()
    df_cassandra.describe()
    df_cassandra.show(25)







  }
}
