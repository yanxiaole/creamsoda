package soda

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

private class LocalScheduler(threads: Int) extends DAGScheduler {
  var attemptId  = new AtomicInteger(0)
  var threadPool = Executors.newFixedThreadPool(threads, DaemonThreadFactory)

  override def submitTasks(tasks: Seq[Task[_]], runId: Int): Unit = {
    def submitTask(task: Task[_], idInJob: Int): Unit = {
      val myAttemptId = attemptId.getAndIncrement()
      threadPool.submit(new Runnable {
        override def run() {
          runTask(task, idInJob, myAttemptId)
        }
      })
    }

    def runTask(task: Task[_], idInJob: Int, attemptId: Int): Unit = {
      logger.info("Running task " + idInJob)
      try {
        val ser = SparkEnv.get.serializer.newInstance()
        val startTime = System.currentTimeMillis
        val bytes = ser.serialize(task)
        val timeTaken = System.currentTimeMillis - startTime
        logger.info("Size of task %d is %d bytes and took %d ms to serialize".format(
          idInJob, bytes.size, timeTaken))
        val deserializedTask = ser.deserialize[Task[_]](bytes)
        val result: Any = deserializedTask.run(attemptId)

        logger.info("Finished task " + idInJob)
        taskEnded(task, Success, result)
      } catch {
        case t: Throwable => {
          logger.error("Exception in task " + idInJob, t)
          // TODO: resubmit the task
        }
      }
    }

    for ((task, i) <- tasks.zipWithIndex) {
      submitTask(task, i)
    }
  }
}
