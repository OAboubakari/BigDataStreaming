import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.ColumnName




object SparkBigData {
  var ss : SparkSession = null

  def main(args: Array[String]): Unit = {
    val session_s = Session_spark(true)

    val df_test = session_s.read
        .format("com.databricks.spark.csv")
        .option("delimiter" , ",")
        .option("header" ,"true")
        .csv("C:\\Users\\PC\\Desktop\\Maîtrisez Spark pour le Big Data avec Scala\\sources de données\\csvs\\2010-12-06.csv")

    //df_test.show(5)
    //val df_2 = df_test.select(col("InvoiceNo").alias("Numero de la Facture") , col("StockCode").cast(IntegerType), col("Quantity"), col("_c0").alias("Id du Client"), col("Description"))
    //df_2.show(5)
    val df3 = df_test.withColumnRenamed("_c0" , "Id_du_Client")
        .withColumn("Total_amount" , round(col("UnitPrice")*col("Quantity") , scale = 2))
        .withColumn("Created_date" , col = current_date())
        .withColumn("Reduction" , when(col("Total_amount")> 15 , lit(3)).otherwise(when(col("Total_amount").between(15,20), lit(3)).otherwise(when(col("Total_amount") < 15, lit(2 )))))
        .withColumn("Net Income" , round(col("Total_amount") - col("Reduction") , scale = 2))


   // df3.show(20)
  //  println("Le nombre de ligne est : " +df3.count())

    // les clients qui n'ont pas reçu de reduction
    //val df_not_reduced = df3.filter(col("Reduction")=== lit(0) && col("Country").isin("United Kingdom","France"))
   // df_not_reduced.show(30)
    //Jointures des dataframes
    //Chargement de la table orders

    val df_orders = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter" , "\t")
      .option("header" ,"true")
      .load("C:\\Users\\PC\\Desktop\\Maîtrisez Spark pour le Big Data avec Scala\\sources de données\\orders.txt")

    //df_orders.show(15)
    // Table products

    val df_products = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter" , "\t")
      .option("header" ,"true")
      .load("C:\\Users\\PC\\Desktop\\Maîtrisez Spark pour le Big Data avec Scala\\sources de données\\product.txt")


    val df_orderlines = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter" , "\t")
      .option("header" ,"true")
      .load("C:\\Users\\PC\\Desktop\\Maîtrisez Spark pour le Big Data avec Scala\\sources de données\\orderline.txt")
     println("=========================================")
      println("Schema de la table orders")
    println("=========================================")
      df_orders.printSchema()
    println("=========================================")
    println("=========================================")
    println("Schema de la table product")
    println("=========================================")
    df_products.printSchema()
    println("=========================================")
    println("=========================================")
    println("Schema de la table orderline")
    println("=========================================")
    df_orderlines.printSchema()
    println("=========================================")


    /* Charger plusieurs dataframes
    val df_group = session_s.read
      .format("csv")
      .option("inferSchema" , ",")
      .option("header" ,"true")
      .load("C:\\Users\\PC\\Desktop\\Maîtrisez Spark pour le Big Data avec Scala\\sources de données\\csvs\\")
    */

    //df_test.show(5)
     //df_test.printSchema()


   // println("df_test_count : "+df_test.count() +"df_group :"+df_group.count() )

  }

  def manipulation_rdd (): Unit = {
    val sc = Session_spark(true).sparkContext
    val session_s = Session_spark(true)
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

    //d'un rdd à partir d'une source de données
    //val rdd2 = sc.textFile("C:\\Users\\PC\\Desktop\\Maîtrisez Spark pour le Big Data avec Scala\\sources\\test_rdd.txt")
    //println("Lecture du contenu de fichier text :")
    //rdd2.foreach(l=> println(l))

    // Transformation des RDD

    //val rdd_trans : RDD[String] = sc.parallelize(List("alain mange une banane","la banane est un bon aliment pour la santé","achetez une bonne banane"))
    //rdd_trans.foreach(l=>println("Ligne de mon rdd : " +l))
    // Utilisation de la fction map
    //val rdd_map = rdd_trans.map(a => a.split(" "))
   // println("Nombre d'elements : "+rdd_map.count())

    // Nombre de caractere

   // val rdd4 = rdd_trans.map( x => (x, x.length , x.contains("banane")))


    //val rdd5 = rdd4.map(x=> (x._1.toUpperCase() , x._3 , x._2))

   // val rdd6 = rdd5.map(x=>(x._1.split(" ") ,1))

    //rdd6.foreach(o => println(o._1(0),o._2))
    // Use of flatmap
    //val rdd_flat = rdd_trans.flatMap(x=>(x.split(" "))).map(x => (x,1))
   // val rdd3 = rdd2.flatMap(x=> x.split(",")).map(m =>(m,1))
    //val rdd_plat = rdd3.flatMap(x=> x.split( " "))
    //rdd3.repartition(1).saveAsTextFile("C:\\Users\\PC\\Desktop\\essai_flat_vers2.txt")
      //rdd_plat.foreach(x => println(x))

   //val rdd_reduce = rdd_trans.flatMap(x => x.split( "  ")).map(w => (w ,1))

   // val rdd_filtered = rdd_reduce.reduceByKey((x ,y ) => x + y)

    //rdd_filtered.foreach(l =>println(l))

//  Manipulation des dataframes

    import session_s.implicits._
    //val df : DataFrame = rdd_filtered.toDF("Texte" ,"Valeur")
    //df.show(2)







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
