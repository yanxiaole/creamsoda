package soda

class Stage(
  val id: Int,
  val rdd: RDD[_]
) {

  override def toString: String = "Stage " + id

  override def hashCode(): Int = id
}
