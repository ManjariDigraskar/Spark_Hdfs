import org.apache.spark.{SparkConf, SparkContext}

object SparkHdfs {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark HDFS")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array(
      ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91),
      ("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62),
      ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78),
      ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
      ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91),
      ("Thomas", "Biology", 74), ("Cory", "Maths", 56), ("Cory", "Physics", 65),
      ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), ("Jackeline", "Maths", 86),
      ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75),
      ("Jackeline", "Biology", 83), ("Juan", "Maths", 63), ("Juan", "Physics", 69),
      ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)), 3)

    val initval = 0
    val agg_rdd = rdd1.map(tu => (tu._1, (tu._2, tu._3)))
    agg_rdd.aggregateByKey(initval)(seqOp, combOp)

    agg_rdd.foreach(println)
      agg_rdd.saveAsTextFile("D:/Aggregate_output")
  }
  def seqOp = (acc: Int, ele: (String, Int)) => if (acc > ele._2) acc else ele._2

  def combOp = (accumulator1: Int, accumulator2: Int) =>
    if (accumulator1 > accumulator2) accumulator1 else accumulator2
}
