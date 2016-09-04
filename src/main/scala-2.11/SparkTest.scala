package test.test

import java.io.{FileWriter, PrintWriter}
import javax.swing.tree.DefaultMutableTreeNode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by D4md1 on 22-Aug-16.
  */
object SparkTest {

  val numberOfClasses: Int = 5
  val measurementAttributes: Int = 14
  val decisionAttribute: Int = 0
  var numberOfMeasurements: Long = 0
  var decisionAttributeEntropy: Double = 0.0
  val printWriter : PrintWriter = new PrintWriter(new FileWriter("result.txt", true))


  val conf = new SparkConf()
    .setMaster("local[1]")
    .setAppName("Spark Test")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    val rdd = sc.textFile(args(0)).cache()
    numberOfMeasurements = rdd.count()
    val measurements = rdd.map(s => getRowsObject(s))
    val classifiedMeasurements = classifyMeasurements(measurements)
    decisionAttributeEntropy = oneAttributeEntropy(classifiedMeasurements,decisionAttribute)

    val maxGain = new DefaultMutableTreeNode((158,getAttributeWithMaximumGain(classifiedMeasurements, decisionAttributeEntropy)))


    nodeIteration(classifiedMeasurements, maxGain)
    val rootOfTree = new DefaultMutableTreeNode((0,0.0,0.0))
    rootOfTree.add(maxGain)

    //rootOfTree.add(new DefaultMutableTreeNode((UUID.randomUUID(), "test")))

    writeTree(rootOfTree)

    //maxGainList.foreach(v => println(v))
  }

  def nodeIteration(classifiedMeasurements: RDD[ClassifiedMeasurement], maxGain: DefaultMutableTreeNode): Unit = {

    val userObject = maxGain.getUserObject.asInstanceOf[(Int,(Int, Double, Double))]

    //var maxGainList = mutable.MutableList[(Int, (Int, Double, Double))]()
    for (i <- 0 until numberOfClasses) {
      val filteredMeasurements = classifiedMeasurements.filter(v => v.values(userObject._2._1) == i)
      val newNode = new DefaultMutableTreeNode((i, (getAttributeWithMaximumGain(filteredMeasurements, userObject._2._2))))
      //writeTree(maxGain)
      maxGain.add(newNode)
      if(userObject._2._2 > 0.0) {
        nodeIteration(filteredMeasurements, newNode)
      }

    }
  }

  def printTree(rootOfTree: DefaultMutableTreeNode): Unit = {
    val en = rootOfTree.breadthFirstEnumeration()
    while(en.hasMoreElements) {
      val node = en.nextElement().asInstanceOf[DefaultMutableTreeNode]
      println((node, node.getParent))
    }
  }

  def writeTree(rootOfTree: DefaultMutableTreeNode): Unit = {
    val en = rootOfTree.breadthFirstEnumeration()
    while(en.hasMoreElements) {
      val node = en.nextElement().asInstanceOf[DefaultMutableTreeNode]
      printWriter.append((node, node.getParent).toString())
    }
  }

  def getAttributeWithMaximumGain(classifiedMeasurements: RDD[ClassifiedMeasurement], parentEntropy: Double): (Int, Double, Double) = {
    var maximumGain = (0,0.0, 0.0)
    for (i<-1 until measurementAttributes) {
      val entropyOverDecisionAttribute = getEntropyOverDecisionAttribute(classifiedMeasurements, i)
      val gain = parentEntropy - entropyOverDecisionAttribute
      if (gain > maximumGain._2) {
        maximumGain = (i, entropyOverDecisionAttribute, gain)
      }
    }
    maximumGain
  }

  def getEntropyOverDecisionAttribute(classifiedMeasurements: RDD[ClassifiedMeasurement], attribute: Int): Double = {
    classifiedMeasurements
      .map(v => ((v.values(attribute), v.values(decisionAttribute)), 1))
      .reduceByKey((v1, v2) => v1 + v2)
      .map(v => (v._1, v._2 / numberOfMeasurements.toDouble))
      .groupBy(v => v._1._1)
      .map(v => v._2
        .map(v => (v._2, entropy(v._2)))
        .reduce((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)))
      .map(v => v._1 * v._2).sum()
  }

  def getAttributeWithMaximumEntropy(classifiedMeasurements: RDD[ClassifiedMeasurement]) : (Int, Double) = {
    var max = (0,0.0)
    for(i<- 0 until measurementAttributes ) {
      val entropy = oneAttributeEntropy(classifiedMeasurements, i)
      if(entropy > max._2){
        max = (i, entropy)
      }
    }
    max
  }

  def oneAttributeEntropy(classifiedMeasurements :RDD[ClassifiedMeasurement], attribute: Int): Double = {
    getProbabilityColumn(classifiedMeasurements, attribute).map(v => entropy(v._3)).sum
  }

  def entropy(probability: Double): Double = {
    if (probability == 0.0) 0.0 else -probability * math.log(probability) / math.log(math.E)
  }

  def getProbabilitiesByAttribute(probabilityRDD: RDD[(Int, Int, Double)]): RDD[(Int, Iterable[(Int, Int, Double)])] = {
    probabilityRDD.groupBy(v => v._1)
  }

  def getProbabilityRDD(classifiedMeasurements: RDD[ClassifiedMeasurement]): RDD[(Int, Int, Double)] = {
    var temp: RDD[(Int, Int, Double)] = sc.emptyRDD
    for(i<- 0 until measurementAttributes ) {
      temp = temp.union(getProbabilityColumn(classifiedMeasurements, i))
    }
    temp
  }

  def getProbabilityColumn(classifiedMeasurements: RDD[ClassifiedMeasurement], column: Int): RDD[(Int, Int, Double)] = {
    val classifiedValuesOfColumn = getClassifiedValuesOfTheColumn(classifiedMeasurements, column)
    classifiedValuesOfColumn
      .map(v=> (v,1))
      .reduceByKey((v1,v2) => v1 + v2)
      .map(v => (column ,v._1, v._2/numberOfMeasurements.toDouble))
  }
  def classifyMeasurements(measurements: RDD[Measurement]): RDD[ClassifiedMeasurement] = {
    var temp: RDD[(Long, (Int, Int))] = sc.emptyRDD
    for(i<- 0 until measurementAttributes ) {
      val classifiedColumn = classifyColumn(measurements, i)
      val map: RDD[(Long, (Int, Int))] = classifiedColumn.zipWithIndex().map { case (k, v) => (v, (i, k)) }
      temp = temp.union(map)
    }
    temp.groupByKey().map(k => k._2.toArray).map(a => mapToClassifiedMeasurement(a))
  }

  def mapToClassifiedMeasurement(array: Array[(Int,Int)]): ClassifiedMeasurement = {
    val temp:Array[Int] = new Array[Int](measurementAttributes)
    array.foreach(v =>temp(v._1) = v._2)
    new ClassifiedMeasurement(temp)
  }


  def classifyColumn(measurements: RDD[Measurement], column: Int): RDD[Int] = {
    val columnValues = getValuesOfTheColumn(measurements, column)
    val max = columnValues.max()
    val min = columnValues.min()
    val step = (max - min)/ numberOfClasses

    columnValues.map(v => getClassOfTheValue(v, max, min, step))
  }

  def getClassOfTheValue(v: Double, max: Double, min: Double, step: Double): Int = {
    for (i <- 1 to numberOfClasses)
      if(v < (min + (i * step)))
        return i - 1
    1
  }

  def getValuesOfTheColumn(measurements: RDD[Measurement], columnNumber: Int): RDD[Double] = {
    measurements.map(m => m.values(columnNumber))
  }

  def getClassifiedValuesOfTheColumn(classifiedMeasurements: RDD[ClassifiedMeasurement], columnNumber: Int): RDD[Int] = {
    classifiedMeasurements.map(m => m.values(columnNumber))
  }

  def getRowsObject(s: String): Measurement = {
    new Measurement(s.split(",").map(s => MyUtils.fixDouble(s)))
  }
}
