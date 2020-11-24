package soda

import com.typesafe.scalalogging.LazyLogging

class ShuffleMapTask(
    runId: Int,
    stageId: Int,
    rdd: RDD[_],
    dep: ShuffleDependency[_],
    val partition: Int,
    locs: Seq[String])
  extends DAGTask[String](runId, stageId) with LazyLogging {

  override def run(id: Int): String = ???
}
