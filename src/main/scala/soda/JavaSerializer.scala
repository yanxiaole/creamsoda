package soda

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}

class JavaSerializationStream(out: OutputStream) extends SerializationStream {
  val objOut = new ObjectOutputStream(out)
  override def writeObject[T](t: T): Unit = { objOut.writeObject(t) }
  override def flush(): Unit = { objOut.flush() }
  override def close(): Unit = { objOut.close() }
}

class JavaDeserializationStream(in: InputStream) extends DeserializationStream {
  val objIn = new ObjectInputStream(in)
  override def readObject[T](): T = objIn.readObject().asInstanceOf[T]
  override def close(): Unit = { objIn.close() }
}

class JavaSerializerInstance extends SerializerInstance {
  override def serialize[T](t: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = outputStream(bos)
    out.writeObject(t)
    out.flush()
    out.close()
    bos.toByteArray
  }

  override def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val in = inputStream(bis)
    in.readObject[T]()
  }

  override def outputStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s)
  }

  override def inputStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s)
  }
}

class JavaSerializer extends Serializer {
  override def newInstance(): SerializerInstance = new JavaSerializerInstance
}
