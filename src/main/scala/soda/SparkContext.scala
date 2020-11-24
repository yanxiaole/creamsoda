package soda

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag

class SparkContext(
    master: String,
    appName: String)
  extends LazyLogging {

  val env: SparkEnv = SparkEnv.create(true)
  SparkEnv.set(env)

  private val scheduler = {
    new LocalScheduler(2)
  }

  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism ): RDD[T] = {
    new ParallelCollection[T](this, seq, numSlices)
  }

  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, rdd.splits.indices)
  }

  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int]
      ): Array[U] = {
    logger.info("Starting job...")
    val start = System.nanoTime()
    val result = scheduler.runJob(rdd, func, partitions)
    logger.info("Job finished in " + (System.nanoTime() - start) / 1e9 + " s")
    result
  }

  def defaultParallelism: Int = 4

  private var nextRddId = new AtomicInteger(0)
  private var nextShuffleId = new AtomicInteger(0)

  private[soda] def newShuffleId(): Int = {
    nextShuffleId.getAndIncrement()
  }

  private[soda] def newRddId(): Int = {
    nextRddId.getAndIncrement()
  }
}

object SparkContext {
  implicit def rddToPairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) =
    new PairRDDFunctions[K, V](rdd)

}