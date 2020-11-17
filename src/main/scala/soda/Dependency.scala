package soda

abstract class Dependency[T](val rdd: RDD[T]) extends Serializable

abstract class NarrowDependency[T](rdd: RDD[T]) extends Dependency(rdd) {
  def getParents(outputPartition: Int): Seq[Int]
}

abstract class ShuffleDependency[T](rdd: RDD[T]) extends Dependency(rdd)

class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency(rdd) {
  override def getParents(partitionId: Int): Seq[Int] = List(partitionId)
}
