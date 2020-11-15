package soda

import java.io.{InputStream, OutputStream}

trait Serializer {
  def newInstance(): SerializerInstance
}

/**
 * An instance of the serializer, for use by one thread at a time
 */
trait SerializerInstance {
  def serialize[T](t: T): Array[Byte]
  def deserialize[T](bytes: Array[Byte]): T
  def outputStream(s: OutputStream): SerializationStream
  def inputStream(s: InputStream): DeserializationStream
}

trait SerializationStream {
  def writeObject[T](t: T): Unit
  def flush(): Unit
  def close(): Unit
}

trait DeserializationStream {
  def readObject[T](): T
  def close(): Unit
}
