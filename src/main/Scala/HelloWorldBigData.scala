import org.apache.velocity.test.provider.Person

object HelloWorldBigData {


  def main(args: Array[String]): Unit = {
  //  println("Hello World : Mon premier script en  Scala pour Data Ingenieur")
  }

  /*Test de variables immutables */

  //val test1 : Int = 25

  //val test_2 = test1 + 21
  //println(test_2)

  /*Test de variables mutables */

 // var test = 35

  //println("La valeur de la variable mutable test est :" +(test + 50) )

/* Usage de la fonction map*/

  val entier = List(1,2,3,4,5,6,7,8,9)
   val double_entier = entier.map(_*2)

  println(double_entier)

  val states = Map(
    "AK" -> "Alaska",
    "IL" -> "Illinois",
    "KY" -> "Kentucky")

    states.foreach(l => println(l))
}
