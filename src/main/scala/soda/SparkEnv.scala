package soda

class SparkEnv (
  val serializer: Serializer
)

object SparkEnv {
  @volatile private var env: SparkEnv = _

  def set(e: SparkEnv): Unit = {
    env = e
  }

  /**
   * Returns the SparkEnv.
   */
  def get: SparkEnv = {
    env
  }

  def create(): SparkEnv = {
    val serializerClass =
      System.getProperty("soda.serializer", "soda.JavaSerializer")
    val serializer = Class.forName(serializerClass, true, Thread.currentThread().getContextClassLoader).newInstance().asInstanceOf[Serializer]

    new SparkEnv(serializer)
  }
}