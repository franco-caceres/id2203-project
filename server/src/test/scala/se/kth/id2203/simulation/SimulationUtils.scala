package se.kth.id2203.simulation

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import se.kth.id2203.kvstore.{Cas, Get, Put}
import se.kth.id2203.simulation.SimulationUtils.isLinearizable

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
    if(!e.isCall) {
      return false
    }
    for(f <- h.events) {
      if(f == e) {
        return true
      }
      if(!f.isCall) {
        return false
      }
    }
    true
  }

  def removeCallAndReturn(e: ExecutionEvent, h:History): History = {
    if(!e.isCall) {
      return h
    }
    val h2 = History(h.events.filterNot(x => {
      if(x.isCall) {
        x.op.id == e.op.id
      } else {
        x.res.id == e.op.id
      }
    }))
    h2
  }

  def isComplete(h: History): Boolean = {
    !h.events.exists(x => {
      x.isCall &&
        h.events.count(y => !y.isCall && x.op.id == y.res.id) == 0
    })
  }

  def removeIncompleteOperations(h: History): History = {
    val completeOperations = h.events.filter(x => {
        !x.isCall || (x.isCall &&
        h.events.count(y => !y.isCall && x.op.id == y.res.id) == 1)
    })
    History(completeOperations)
  }

  def isLinearizable(h: History, S: collection.mutable.Map[String, String] = collection.mutable.Map.empty): Boolean = {
    /*if(!isComplete(h)) {
      throw new IllegalArgumentException("Only checking complete histories for linearizability")
    }*/
    _isLinearizable(h, S)
  }

  def _isLinearizable(h: History, S: collection.mutable.Map[String, String] = collection.mutable.Map.empty): Boolean = {
    if(h.events.isEmpty) {
      return true
    }
    for(e <- h.events) {
      if(isMinimal(e, h)) {
        val eResOpt = h.events.find(x => !x.isCall && x.res.id == e.op.id)
        if(eResOpt.isDefined) {
          val eRes = eResOpt.get
          val originalValue = S.get(e.op.key)
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
          if(ok && _isLinearizable(removeCallAndReturn(e, h), S)) {
            return true
          } else {
            if(originalValue.isEmpty) {
              S.remove(e.op.key)
            } else {
              S(e.op.key) = originalValue.get
            }
          }
        } else {
          // return event does not exist
          // either remove it
          if(_isLinearizable(removeCallAndReturn(e, h), S)) {
            return true
          }
          // or apply it
          val originalValue = S.get(e.op.key)
          e.op match {
            case g: Get => {
              true
            }
            case p: Put => {
              S(p.key) = p.value
            }
            case c: Cas => {
              if(S.get(c.key).isDefined && S(c.key) == c.compareValue) {
                S(c.key) = c.setValue
              }
            }
          }
          if(_isLinearizable(removeCallAndReturn(e, h), S)) {
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
    }
    false
  }
}
