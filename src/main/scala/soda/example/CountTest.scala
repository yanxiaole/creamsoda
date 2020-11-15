package soda.example

import com.typesafe.scalalogging.LazyLogging
import soda.SparkContext

object CountTest extends LazyLogging {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: CountTest <host> [numMappers]")
      System.exit(1)
    }

    val numMappers = if (args.length > 1) args(1).toInt else 2

    val sc = new SparkContext(args(0), "GroupBy Test")

    val pair =
      sc.parallelize(0 until numMappers, numMappers)
        .map { p => p + 1 }
    println(pair.count)

    System.exit(0)
  }

}
