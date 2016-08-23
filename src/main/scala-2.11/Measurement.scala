package test.test

/**
  * Created by D4md1 on 22-Aug-16.
  */
class Measurement(array: Array[Double]) {
  //println(array(0))
  def values = array
  override def toString: String = array.mkString(" | ") + "  ::this is a measurement object"
}
