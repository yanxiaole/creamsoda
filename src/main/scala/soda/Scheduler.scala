package soda

import scala.reflect.ClassTag

trait Scheduler {

  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int]): Array[U]
}
