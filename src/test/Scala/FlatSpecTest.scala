import org.scalatest.flatspec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import org.scalatest.matchers.Matcher
import org.scalatest.Assertions._
//org.scalatest.matchers.should.Matchers



class FlatSpecTest  extends AnyFlatSpec  {

  "la division" should ("renvoyer 10 ") in {
  assert(HelloWorldBigData.division(40,2) == 20)
  }


  "An arithmetic error" should("be thrown ") in (HelloWorldBigData.division(40,1))



}
