import org.scalatest.flatspec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import org.scalatest.matchers.Matcher
import org.scalatest.Assertions._

class SparkTestunitaires extends AnyFlatSpec {

  it should("instanciate a Spark session") in {

    var Env : Boolean = true
    val ss_test = SparkBigData.Session_spark(Env)
  }




}
