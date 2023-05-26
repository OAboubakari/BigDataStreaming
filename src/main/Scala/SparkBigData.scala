import org.apache.spark.rdd.RDD
import org.apache.spark.sql._


object SparkBigData {
  var ss : SparkSession = null

  def main(args: Array[String]): Unit = {
    val sc = Session_spark(true).sparkContext
    val rdd_test : RDD[String] = sc.parallelize(List("Ouedraogo","Djamila","Yennega"))
    rdd_test.foreach{
      l => println(l)
    }

  }
  /**
   * Fonction qui initialise et instancie une session spark
   * @param Env : Une variable boolean qui indique l'environnement sur lequel notre application est deployée
   *            Si Env = true , alors l'appli est en local , otherwise elle est en production
   * @return return le resultat de la fonction
   */


  def Session_spark (Env : Boolean = true) : SparkSession = {
    if(Env == true)
      {
        System.setProperty("hadoop.home.dir" , "C:/Hadoop/")
        ss = SparkSession.builder()
            .master("local[*]")
            .config("spark.serializer" ,"org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.CrossJoin.enabled" , "true")
            .getOrCreate()
//           .enableHiveSupport()

      }
    else
      {
        ss = SparkSession.builder()
        .appName("Mon application Spark")
        .config("spark.serializer" ,"org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.CrossJoin.enabled" , "true")
        .enableHiveSupport()
        .getOrCreate()


      }
    return ss





  }






}
