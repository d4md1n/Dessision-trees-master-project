package test.test
/**
  * Created by D4md1 on 22-Aug-16.
  */
object MyUtils {

  def printUtil(s: String){
    println(s"$s ::::::this has been printed by a util function")
  }

  def fixDouble (value: String) :Double = {
    var variable = value
    if (value.startsWith(".")) {
      variable = 0 + value
    }
    variable.toDouble
  }
}
