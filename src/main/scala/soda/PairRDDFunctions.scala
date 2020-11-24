package soda

import scala.reflect.ClassTag

import com.typesafe.scalalogging.LazyLogging

class PairRDDFunctions[K: ClassTag, V: ClassTag](
    self: RDD[(K, V)])
  extends LazyLogging
  with Serializable {

  def reduceByKey(func: (V, V) => V, numSplit: Int): RDD[(K, V)] = {
    new ShuffledRDD(self, func)
  }

}
