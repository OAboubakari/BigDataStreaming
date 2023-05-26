import org.apache.spark.rdd.RDD
import org.apache.spark.sql._


object SparkBigData {
  var ss : SparkSession = null

  def main(args: Array[String]): Unit = {
    val sc = Session_spark(true).sparkContext
    sc.setLogLevel("OFF")
    /** Creation d'un rdd à partir d'une List
    val rdd_test : RDD[String] = sc.parallelize(List("Ouedraogo","Djamila","Yennega","Djibril","Mamadi","Kassoum","Konate"))
    rdd_test.foreach{
      l => println(l)
    }
    //Creation d'un rdd à partir d'un tableau

    val rdd_test1 : RDD[String] = sc.parallelize(Array("Kone","Moussa","Ladji"))
    rdd_test1.foreach{
      l => println(l)
    }
    //Creation d'un rdd à partir d'une seq(sequence)

    val mon_rdd  = sc.parallelize(Seq(("Ouedraogo" , "Math" , 15),("Diomandé" , "Math", 14),("Sylla","Math", 18),("Manassé" ,"Math",19)))
    println("Mon premier element :")
      mon_rdd.take(1).foreach(l=> println(l))

   // println("Tous les elements :")
   // mon_rdd.take(4).foreach(l=> println(l))

    // Enregistrer sur disk
    mon_rdd.repartition(1).saveAsObjectFile("C:\\Users\\PC\\Desktop\\mon test.txt") */

    /** Creation d'un rdd à partir d'une source de données
    val rdd2 = sc.textFile("C:\\Users\\PC\\Desktop\\Maîtrisez Spark pour le Big Data avec Scala\\sources\\test_rdd.txt")
    println("Lecture du contenu de fichier text :")
    rdd2.foreach(l=> println(l))
    */

// Le lineage ou  (plan d'execution)




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
