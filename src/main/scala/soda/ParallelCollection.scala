package soda

import scala.reflect.ClassTag

class ParallelCollectionSplit[T: ClassTag](
    val rddId: Long,
    val slice: Int,
    values: Seq[T])
  extends Split with Serializable {

  def iterator: Iterator[T] = values.iterator

  override val index: Int = slice
}

class ParallelCollection[T: ClassTag](
    sc: SparkContext,
    data: Seq[T],
    numSlices: Int)
  extends RDD[T](sc) {

  private val splits_ = {
    val slices = ParallelCollection.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionSplit(id, i, slices(i))).toArray
  }

  override def splits: Array[Split] = splits_.asInstanceOf[Array[Split]]

  override def compute(s: Split): Iterator[T] =
    s.asInstanceOf[ParallelCollectionSplit[T]].iterator

}

private object ParallelCollection {
  def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of slices required")
    }
    seq match {
      case r: Range => {
        (0 until numSlices).map(i => {
          val start = ((i * r.length.toLong) / numSlices).toInt
          val end = (((i+1) * r.length.toLong) / numSlices).toInt
          new Range(r.start + start * r.step, r.start + end * r.step, r.step)
        }).asInstanceOf[Seq[Seq[T]]]
      }
      case _ => {
        val array = seq.toArray
        (0 until numSlices).map(i => {
          val start = ((i * array.length.toLong) / numSlices).toInt
          val end = (((i+1) * array.length.toLong) / numSlices).toInt
          array.slice(start, end).toSeq
        })
      }
    }
  }
}