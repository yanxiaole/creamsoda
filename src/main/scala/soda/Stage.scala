package soda

class Stage(val id: Int) {

  override def toString: String = "Stage " + id

  override def hashCode(): Int = id
}
