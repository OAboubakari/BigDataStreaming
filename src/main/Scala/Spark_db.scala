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

    val session_db_mysql = Session_spark(true)
    // les proprietés de connexion

    val propriete_connexion = new Properties()
    propriete_connexion.put("user","consultant")
    propriete_connexion.put("password", "Spark#87")

    // la chaine de connexion

    val df_mysql = session_db_mysql.read.jdbc("jdbc:mysql://127.0.0.1:3306/spark_db?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC",
      table = "spark_db.orders", propriete_connexion)
    df_mysql.show(50)


  }

}
