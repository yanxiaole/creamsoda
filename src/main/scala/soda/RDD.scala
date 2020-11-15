package soda

import scala.reflect.ClassTag

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
 * partitioned collection of elements that can be operated on in parallel.
 *
 * Each RDD is characterized by five main properties:
 * - A list of splits (partitions)
 * - A function for computing each split
 * - A list of dependencies on other RDDs
 * - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 * - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
 *   HDFS)
 *
 * All the scheduling and execution in Spark is done based on these methods, allowing each RDD to
 * implement its own way of computing itself.
 *
 * This class also contains transformation methods available on all RDDs (e.g. map and filter). In
 * addition, PairRDDFunctions contains extra methods available on RDDs of key-value pairs, and
 * SequenceFileRDDFunctions contains extra methods for saving RDDs to Hadoop SequenceFiles.
 */
abstract class RDD[T: ClassTag](@transient sc: SparkContext) extends Serializable {

  def splits: Array[Split]
  def compute(split: Split): Iterator[T]

  def context: SparkContext = sc

  val id: Int = sc.newRddId()

  final def iterator(split: Split): Iterator[T] = {
    compute(split)
  }

  def map[U: ClassTag](f: T => U): RDD[U] = new MappedRDD[U, T](this, f)

  def count(): Long = {
    sc.runJob(this, (iter: Iterator[T]) => {
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next
      }
      result
    }).sum
  }
}

class MappedRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],
    f: T => U)
  extends RDD[U](prev.context) {

  override def splits: Array[Split] = prev.splits
  override def compute(split: Split): Iterator[U] = prev.compute(split).map(f)
}