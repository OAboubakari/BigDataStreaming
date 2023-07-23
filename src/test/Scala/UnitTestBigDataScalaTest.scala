import org.scalatest.funsuite
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest
import org.scalatest.Assertions._

class UnitTestBigDataScalaTest  extends AnyFunSuite {

  test("la division doit renvoyer 10")
  {

    assert(HelloWorldBigData.division(40,2) == 20)

  }

}

