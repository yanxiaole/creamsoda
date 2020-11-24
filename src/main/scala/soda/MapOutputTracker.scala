package soda

import java.util.concurrent.ConcurrentHashMap

class MapOutputTracker() {

  private var serverUris = new ConcurrentHashMap[Int, Array[String]]()

  def getServerUris(shuffleId: Int): Array[String] = ???

  def registerMapOutputs(shuffleId: Int, locs: Array[String]): Unit = {
    serverUris.put(shuffleId, Array[String]() ++ locs)
  }

  def registerShuffle(shuffleId: Int, numMaps: Int): Unit = {
    if (serverUris.get(shuffleId) != null) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
    serverUris.put(shuffleId, new Array[String](numMaps))
  }

}
