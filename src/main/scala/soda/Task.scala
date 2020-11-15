package soda

abstract class Task[T] extends Serializable {
  def run(id: Int): T
}
