import java.util._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.plans
import SparkBigData._

object Spark_db {


  def main(args: Array[String]): Unit = {

    val session_db = Session_spark(true)
    // les proprietés de connexion

    val propriete_connexion = new Properties()
    propriete_connexion.put("user","consultant")
    propriete_connexion.put("password", "Spark#87")

    // la chaine de connexion

   // val df_mysql = session_db_mysql.read.jdbc("jdbc:mysql://127.0.0.1:3306/spark_db?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC",
   //   table = "spark_db.orders", propriete_connexion)

   //df_mysql.show(230)

    //Connexion avec une base de donnees PostgreSql

   // val postgre_propertie = new  Properties()

   // postgre_propertie.put("user" , "postgres")
   // postgre_propertie.put("password" , "Starter@1987A")

    //La chaine de connexion


   // val df_postgre = session_db.read.jdbc("jdbc:postgresql://127.0.0.1:5432/spark_db?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC",
    //  table = "orders", postgre_propertie)

    //df_postgre.show(50)

    //Connexion avec une base de donnees SQLserver

    /** val sqlserver_propertie = new  Properties()

    sqlserver_propertie.put("user" , "SMARTDATAINSTIT\\PC")
    sqlserver_propertie.put("password" , "")

    val df_sql_server = session_db.read.jdbc("jdbc:sqlserver://SMARTDATAINSTIT\\SPARKSQLSERVER:1433; databaseName=spark_db;" , table = "orders" , sqlserver_propertie)
    df_sql_server.show(5)
    */





  }

}
