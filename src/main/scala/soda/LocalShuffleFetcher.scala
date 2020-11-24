package soda

import java.io.FileInputStream

import scala.collection.mutable.{ArrayBuffer, HashMap}

import com.typesafe.scalalogging.LazyLogging

class LocalShuffleFetcher extends ShuffleFetcher with LazyLogging {
  override def fetch[K, V](shuffleId: Int, reduceId: Int, func: (K, V) => Unit): Unit = {
    val ser = SparkEnv.get.serializer.newInstance()
    val splitsByUri = new HashMap[String, ArrayBuffer[Int]]
    val serverUris = SparkEnv.get.mapOutputTracker.getServerUris(shuffleId)

    for ((serverUri, index) <- serverUris.zipWithIndex) {
      splitsByUri.getOrElseUpdate(serverUri, ArrayBuffer()) += index
    }

    for ((serverUri, inputIds) <- splitsByUri) {
      for (i <- inputIds) {
        val url = "/tmp/%s/shuffle/%d/%d/%d".format(serverUri, shuffleId, i, reduceId)

        var totalRecords = -1
        var recordsProcessed = 0
        var tries = 0
        while (totalRecords == -1 || recordsProcessed < totalRecords) {
          tries += 1
          if (tries > 4) {
            logger.error("Failed to fetch " + reduceId + " four times; giving up")
            throw new FetchFailedException(serverUri, shuffleId, i, reduceId, null)
          }
          var recordsRead = 0
          try {
            val inputStream = ser.inputStream(new FileInputStream(url))
            try {
              totalRecords = inputStream.readObject().asInstanceOf[Int]
              logger.info("Total records to read from " + url + ": " + totalRecords)
              while (true) {
                val pair = inputStream.readObject().asInstanceOf[(K, V)]
                if (recordsRead <= recordsProcessed) {
                  func(pair._1, pair._2)
                  recordsProcessed += 1
                }
                recordsRead += 1
              }
            } finally {
              inputStream.close()
            }
          } catch {
            case other: Exception => {
              logger.error("Fetch failed", other)
              throw new FetchFailedException(serverUri, shuffleId, i, reduceId, other)
            }
          }
        }
        logger.info("Fetched all " + totalRecords + " records successfully")
      }
    }
  }
}
