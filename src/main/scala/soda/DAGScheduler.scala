package soda

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.typesafe.scalalogging.LazyLogging

abstract class DAGTask[T](val runId: Int, val stageId: Int) extends Task[T] {}

case class CompletionEvent(
    task: DAGTask[_],
    reason: TaskEndReason,
    result: Any
)

sealed trait TaskEndReason
case object Success extends TaskEndReason

private trait DAGScheduler extends Scheduler with LazyLogging {

  def submitTasks(tasks: Seq[Task[_]], runId: Int): Unit

  def taskEnded(task: Task[_], reason: TaskEndReason, result: Any): Unit = {
    lock.synchronized {
      val dagTask = task.asInstanceOf[DAGTask[_]]
      eventQueues.get(dagTask.runId) match {
        case Some(queue) =>
          queue += CompletionEvent(dagTask, reason, result)
          lock.notifyAll()
        case None =>
          logger.info("Ignoring completion event for DAG job " + dagTask.runId + " because it's gone")
      }
    }
  }

  val POLL_TIMEOUT = 500L

  private val lock = new Object

  private val eventQueues = new mutable.HashMap[Int, mutable.Queue[CompletionEvent]]

  val env: SparkEnv = SparkEnv.get
  val mapOutputTracker = env.mapOutputTracker

  val nextRunId = new AtomicInteger(0)
  val nextStageId = new AtomicInteger(0)

  val idToStage = new mutable.HashMap[Int, Stage]

  val shuffleToMapStage = new mutable.HashMap[Int, Stage]

  def getShuffleMapStage(shuf: ShuffleDependency[_]): Stage = {
    shuffleToMapStage.get(shuf.shuffleId) match {
      case Some(stage) => stage
      case None =>
        val stage = newStage(shuf.rdd, Some(shuf))
        shuffleToMapStage(shuf.shuffleId) = stage
        stage
    }
  }

  override def runJob[T, U: ClassTag](
      finalRdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int]): Array[U] = {
    lock.synchronized {
      val runId = nextRunId.getAndIncrement()

      val outputParts = partitions.toArray
      val numOutputParts: Int = partitions.size
      val finalStage = newStage(finalRdd, None)
      val results = new Array[U](numOutputParts)
      val finished = new Array[Boolean](numOutputParts)
      var numFinished = 0

      val waiting = new mutable.HashSet[Stage]
      val running = new mutable.HashSet[Stage]
      val pendingTasks = new mutable.HashMap[Stage, mutable.HashSet[Task[_]]]

      logger.info("Final stage: " + finalStage)

      def submitStage(stage: Stage) {
        if (!waiting(stage) && !running(stage)) {
          val missing = getMissingParentStages(stage)
          if (missing == Nil) {
            logger.info("Submitting " + stage + ", which has no missing parents")
            submitMissingTasks(stage)
            running += stage
          } else {
            for (parent <- missing) {
              submitStage(parent)
            }
            waiting += stage
          }
        }
      }

      def submitMissingTasks(stage: Stage) {
        val myPending = pendingTasks.getOrElseUpdate(stage, new mutable.HashSet)
        val tasks = new mutable.ArrayBuffer[Task[_]]()
        if (stage == finalStage) {
          for (id <- 0 until numOutputParts if !finished(id)) {
            val part = outputParts(id)
            tasks += new ResultTask(runId, stage.id, finalRdd, func, part, id)
          }
        } else {
          for (p <- 0 until stage.numPartitions if stage.outputLocs(p) == Nil) {
            val locs = ???
            tasks += new ShuffleMapTask(runId, stage.id, stage.rdd, stage.shuffleDep.get, p, locs)
          }
        }
        myPending ++= tasks
        submitTasks(tasks, runId)
      }

      eventQueues(runId) = new mutable.Queue[CompletionEvent]()
      submitStage(finalStage)

      while (numFinished != numOutputParts) {
        val eventOption = waitForEvent(runId, POLL_TIMEOUT)
        val time = System.currentTimeMillis()

        if (eventOption.isDefined) {
          val evt = eventOption.get
          val stage = idToStage(evt.task.stageId)

          if (evt.reason == Success) {
            logger.info("Completed " + evt.task)
            evt.task match {
              case rt: ResultTask[_, _] =>
                results(rt.outputId) = evt.result.asInstanceOf[U]
                finished(rt.outputId) = true
                numFinished += 1
              case smt: ShuffleMapTask =>
                val stage = idToStage(smt.stageId)
                stage.addOutputLoc(smt.partition, evt.result.asInstanceOf[String])
                if (running.contains(stage) && pendingTasks(stage).isEmpty) {
                  logger.info(stage + " finished, looking for newly runnable stages")
                  running -= stage
                  if (stage.shuffleDep != None) {
                    mapOutputTracker.registerMapOutputs(
                      stage.shuffleDep.get.shuffleId,
                      stage.outputLocs.map(_.head).toArray)
                  }
                  val newlyRunnable = new ArrayBuffer[Stage]
                  for (stage <- waiting if getMissingParentStages(stage) == Nil) {
                    newlyRunnable += stage
                  }
                  waiting --= newlyRunnable
                  running ++= newlyRunnable
                  for (stage <- newlyRunnable) {
                    submitMissingTasks(stage)
                  }
                }
            }
          } else {
            eventQueues -= runId
            throw new SparkException("Task failed: " + evt.task + ", reason: " + evt.reason)
          }
        }
      }

      eventQueues -= runId
      return results
    }
  }

  def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new mutable.HashSet[Stage]
    val visited = new mutable.HashSet[RDD[_]]()
    def visit(rdd: RDD[_]): Unit = {
      if (!visited(rdd)) {
        visited += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case narrowDep: NarrowDependency[_] =>
              visit(narrowDep.rdd)
            case shuffleDep: ShuffleDependency[_] =>
              val stage = getShuffleMapStage(shuffleDep)
              missing += stage
          }
        }
      }
    }

    visit(stage.rdd)
    missing.toList
  }

  def newStage(rdd: RDD[_], shuffleDep: Option[ShuffleDependency[_]]): Stage = {
    if (shuffleDep != None) {
      mapOutputTracker.registerShuffle(shuffleDep.get.shuffleId, rdd.splits.size)
    }
    val id = nextStageId.getAndIncrement()
    val stage = new Stage(id, rdd, shuffleDep)
    idToStage(id) = stage
    stage
  }

  def waitForEvent(runId: Int, timeout: Long): Option[CompletionEvent] = {
    val endTime = System.currentTimeMillis() + timeout
    while (eventQueues(runId).isEmpty) {
      val time = System.currentTimeMillis()
      if (time >= endTime) {
        return None
      } else {
        lock.wait(endTime - time)
      }
    }
    Some(eventQueues(runId).dequeue())
  }
}
