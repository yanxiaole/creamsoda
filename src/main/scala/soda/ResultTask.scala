package soda

class ResultTask[T, U](
    runId: Int,
    stageId: Int,
    rdd: RDD[T],
    func: Iterator[T] => U,
    partition: Int,
    val outputId: Int)
  extends DAGTask[U](runId, stageId) {

  val split: Split = rdd.splits(partition)

  override def toString: String = "ResultTask(" + stageId + ", " + ")"

  override def run(id: Int): U = {
    func(rdd.iterator(split))
  }
}
