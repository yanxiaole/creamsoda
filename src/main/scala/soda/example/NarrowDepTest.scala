package soda.example

import scala.util.Random

import com.typesafe.scalalogging.LazyLogging
import soda.SparkContext

object NarrowDepTest extends LazyLogging {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: NarrowDepTest <host> [numMappers]")
      System.exit(1)
    }

    val numMappers = if (args.length > 1) args(1).toInt else 2
    var numKVPairs = if (args.length > 2) args(2).toInt else 5
    var valSize = if (args.length > 3) args(3).toInt else 5

    val sc = new SparkContext(args(0), "GroupBy Test")

    val pair =
      sc.parallelize(0 until numMappers, numMappers)
        .map { p => p + 10 }
        .map { p => p % 3 }

    println(pair.collect().toList)

    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }
    // Enforce that everything has been calculated and in cache
    println(pairs1.collect().toList)

    System.exit(0)
  }

}
