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
  // le chemin qui recevra les fichiers traités
  val chemin_destination = new Path("C:\\destination_fichier_bano")

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

    //Etape 2

    /** Taf :
     * 1-creer une colonne codedepartement à partir des 2 premiers chiffres de code postal
     * 2 - Creer des correspondance pour le code_source_bno
     *  OSM : pour OpenStreetMap
     *  OO : pour OpenData
     *  O+O : OpenData_OSM
     *  CAD : pour le Cadastre
     *  C+O : Cadastre OpenStreetMap
     */
    // Creation d'une nouvelle variable df

    val df : DataFrame = df_bano
      .withColumn("code_department", substring(col("code_postal"), 1, 2))
      .withColumn("Libelle_source" , when(col("code_source_bano") === lit("OSM"),lit("OpenStreetMap"))
        .otherwise(when(col("code_source_bano") === lit("OO"),lit("OpenData"))
          .otherwise(when(col("code_source_bano") === lit("O+O"),lit("OpenData_OSM"))
            .otherwise(when(col("code_source_bano") === lit("CAD"),lit("Cadastre"))
              .otherwise(when(col("code_source_bano") === lit("C+O"),lit("Cadastre_OSM")))))))




    // Etape 2 : Creation d'un fichier BANO par Departement
    // La liste des departements
    //J'utilise collect() car ici c'est un dataframe(qui est partitionné et reparti entre les noeux du cluster or les list ne le sont pas)


    val df_departements = df.select(col("code_department")).distinct().filter(col("code_department").isNotNull)

    //df.show(5)

    //Methode 1(Version scala classique)
    val liste_departement = df.select(col("code_department"))
      .distinct()
      .filter(col("code_department").isNotNull)
      .collect()
      .map(lst => lst(0)).toList

    //Creation des fichier par departement

    liste_departement.foreach{
       x => df.filter(col("code_department")=== x.toString)
           .coalesce(1) //Executer le calcul sur le noeud principal du cluster avec un seul fichier
           .write
           .format("com.databricks.spark.com")
           .option("delimiter" , ";")
           .option("header", "true")
           .mode(SaveMode.Overwrite)
           .csv("C:\\Users\\PC\\Desktop\\Maîtrisez Spark pour le Big Data avec Scala\\Projet BANO\\Bano_resultat\\departement_Numero " + x.toString)

         val chemin_source = new Path("C:\\Users\\PC\\Desktop\\Maîtrisez Spark pour le Big Data avec Scala\\Projet BANO\\Bano_resultat\\departement_Numero " + x.toString)

        //deplacement des fichiers vers la destination
        fs.copyFromLocalFile(chemin_source,chemin_destination)



    }

    /**
    //Methode 2 : On passe par le dataframe df_departement(Version distribuée)

    df_departements.foreach{
      dep => df.filter(col("code_department")=== dep.toString)
        .repartition(1)
        .write
        .format("com.databricks.spark.com")
        .option("delimiter" , ";")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv("C:\\Users\\PC\\Desktop\\Maîtrisez Spark pour le Big Data avec Scala\\Projet BANO\\Bano_resultat\\departement_Numero " + dep.toString)

        //deplacement des fichiers vers la destination

        val chemin_source = new Path("C:\\Users\\PC\\Desktop\\Maîtrisez Spark pour le Big Data avec Scala\\Projet BANO\\Bano_resultat\\departement_Numero" +dep.toString)

      fs.copyFromLocalFile(chemin_source,chemin_destination)
     */


    }









}
