package se.kth.id2203.simulation

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import se.kth.id2203.kvstore.{Cas, Get, Put}

object SimulationUtils {
  def serialize(value: Any): String = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    new String(
      Base64.getEncoder().encode(stream.toByteArray),
      UTF_8
    )
  }

  def deserialize[T](str: String): T = {
    val bytes = Base64.getDecoder().decode(str.getBytes(UTF_8))
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject()
    ois.close
    value.asInstanceOf[T]
  }

  def isMinimal(e: ExecutionEvent, h: History): Boolean = {
    if(!e.call) {
      return false
    }
    for(f <- h.events) {
      if(f == e) {
        return true
      }
      if(!f.call) {
        return false
      }
    }
    true
  }

  def removeCallAndReturn(e: ExecutionEvent, h:History): History = {
    if(!e.call) {
      return h
    }
    val h2 = History(h.events.filterNot(x => {
      if(x.call) {
        x.op.id == e.op.id
      } else {
        x.res.id == e.op.id
      }
    }))
    h2
  }

  def isLinearizable(h: History, S: collection.mutable.Map[String, String] = collection.mutable.Map.empty): Boolean = {
    if(h.events.isEmpty) {
      return true
    }
    for(e <- h.events) {
      if(isMinimal(e, h)) {
        val eRes = h.events.find(x => !x.call && x.res.id == e.op.id).get
        var originalValue = S.get(e.op.key)
        val ok: Boolean = e.op match {
          case g: Get => {
            val expected = S.get(g.key)
            val actual = eRes.res.value
            expected == actual
          }
          case p: Put => {
            S(p.key) = p.value
            val expected = p.value
            val actual = eRes.res.value
            Some(expected) == actual
          }
          case c: Cas => {
            if(S.get(c.key).isDefined && S(c.key) == c.compareValue) {
              S(c.key) = c.setValue
            }
            val expected = S.get(c.key)
            val actual = eRes.res.value
            expected == actual
          }
        }
        if(ok && isLinearizable(removeCallAndReturn(e, h), S)) {
          return true
        } else {
          if(originalValue.isEmpty) {
            S.remove(e.op.key)
          } else {
            S(e.op.key) = originalValue.get
          }
        }
      }
    }
    false
  }
}
