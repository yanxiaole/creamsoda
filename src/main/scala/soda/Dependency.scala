package soda

abstract class Dependency[T](val rdd: RDD[T], val isShuffle: Boolean) extends Serializable

abstract class NarrowDependency[T](rdd: RDD[T]) extends Dependency(rdd, false) {
  def getParents(outputPartition: Int): Seq[Int]
}

class ShuffleDependency[T](
    val shuffleId: Int,
    rdd: RDD[T])
  extends Dependency(rdd, true)

class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency(rdd) {
  override def getParents(partitionId: Int): Seq[Int] = List(partitionId)
}
