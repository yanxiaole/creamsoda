package soda

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
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

  val nextRunId = new AtomicInteger(0)
  val nextStageId = new AtomicInteger(0)

  val idToStage = new mutable.HashMap[Int, Stage]

  override def runJob[T, U: ClassTag](
      finalRdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int]): Array[U] = {
    lock.synchronized {
      val runId = nextRunId.getAndIncrement()

      val outputParts = partitions.toArray
      val numOutputParts: Int = partitions.size
      val finalStage = newStage()
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
          // TODO
          logger.error("not implemented")
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
              case _ => ???
            }

          } else {

          }

        }

      }

      eventQueues -= runId
      return results
    }
  }

  def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new mutable.HashSet[Stage]
    missing.toList
  }

  def newStage(): Stage = {
    val id = nextStageId.getAndIncrement()
    val stage = new Stage(id)
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
