import org.junit._
import org.junit.Assert._
import junit.framework.TestCase

class UnitTestBigDataUnit {
  @Test
  def testDivision () : Unit = {
    var val_actuelle : Double = HelloWorldBigData.division(15000,20)
    var val_attendue : Int = 750
    assertEquals("la valeure attendue est 15 ",val_attendue,val_actuelle.toInt)


  }


}
