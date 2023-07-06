import org.apache.spark.sql._
import org.apache.spark.sql.types
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.datasources._
import org.apache.hadoop.hbase._

import SparkBigData._


object Spark_Hbase {

  // Definition du catalogue pour le mapping entre les Dataframes et  tables Hbases

  def catalogue_hbase(): String = """{
                                    |        |"table":{"namespace":"default", "name":"table_orders"},
                                    |        |"rowkey":"key",
                                    |        |"columns":{
                                    |          |"orderid":{"cf":"rowkey", "col":"key", "type":"string"},
                                    |          |"customer_id":{"cf":"orders", "col":"customerid", "type":"string"},
                                    |          |"campaign_id":{"cf":"orders", "col":"campaignid", "type":"string"},
                                    |          |"orderdate":{"cf":"orders", "col":"orderdate", "type":"string"},
                                    |          |"city":{"cf":"orders", "col":"city", "type":"string"},
                                    |          |"state":{"cf":"orders", "col":"state", "type":"string"},
                                    |          |"zipcode":{"cf":"orders", "col":"zipcode", "type":"string"},
                                    |          |"paymenttype":{"cf":"orders", "col":"paymenttype", "type":"string"},
                                    |          |"totalprice":{"cf":"orders", "col":"totalprice", "type":"string"}
                                    |		  |"numorderlines":{"cf":"orders", "col":"numorderlines", "type":"string"}
                                    |        |}
                                    |      |}
                                    |	  """.stripMargin


  def main(args: Array[String]): Unit = {



    
  }

}
