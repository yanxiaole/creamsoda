package soda

abstract class ShuffleFetcher {

  def fetch[K, V](shuffleId: Int, reduceId: Int, func: (K, V) => Unit)

}
