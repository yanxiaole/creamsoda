package soda

class Stage(
    val id: Int,
    val rdd: RDD[_],
    val shuffleDep: Option[ShuffleDependency[_]]) {

  val isShuffleMap = shuffleDep.isDefined
  val numPartitions = rdd.splits.size
  val outputLocs = Array.fill[List[String]](numPartitions)(Nil)
  var numAvailableOutputs = 0

  def addOutputLoc(partition: Int, host: String): Unit = {
    val prevList = outputLocs(partition)
    outputLocs(partition) = host :: prevList
    if (prevList == Nil)
      numAvailableOutputs += 1
  }

  override def toString: String = "Stage " + id

  override def hashCode(): Int = id
}
