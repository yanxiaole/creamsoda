package soda

import java.util.{HashMap => JHashMap}

class ShuffledRDD[K, V, C](
    parent: RDD[(K, V)],
    aggregator: (C, C) => C)
  extends RDD[(K, C)](parent.context) {

  override def splits: Array[Split] = ???

  val dep = new ShuffleDependency[(K, V)](context.newShuffleId(), parent)
  override val dependencies: List[Dependency[_]] = List(dep)

  override def compute(split: Split): Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    def mergePair(k: K, c: C): Unit = {
      val oldC = combiners.get(k)
      if (oldC == null) {
        combiners.put(k, c)
      } else {
        combiners.put(k, aggregator(oldC, c))
      }
    }

    val fetcher = new LocalShuffleFetcher()
    fetcher.fetch(dep.shuffleId, split.index, mergePair)

    new Iterator[(K, C)] {
      var iter = combiners.entrySet().iterator()

      override def hasNext(): Boolean = iter.hasNext()
      override def next(): (K, C) = {
        val entry = iter.next()
        (entry.getKey, entry.getValue)
      }
    }
  }
}
