package se.kth.id2203.simulation

import java.util.UUID

import org.scalatest.{FunSuite, Matchers}
import se.kth.id2203.kvstore._

class SimulationUtilsTest extends FunSuite with Matchers {

  /*
  *
  * p1:-|-GET(key)==a-|----------------
  * p2:------------------|-PUT(key,a)-|
  *
  * */
  test("Scenario 1 should not be linearizable") {
    val uuidGet = UUID.randomUUID()
    val reqGet = Get("key", uuidGet)
    val resGet = OperationResponse(uuidGet, OpCode.Ok, Some("a"))
    val callGet = ExecutionEvent(1, call = true, reqGet, null)
    val returnGet = ExecutionEvent(2, call = false, null, resGet)

    val uuidPut = UUID.randomUUID()
    val reqPut = Put("key", "a", uuidPut)
    val resPut = OperationResponse(uuidPut, OpCode.Ok, Some("a"))
    val callPut = ExecutionEvent(3, call = true, reqPut, null)
    val returnPut = ExecutionEvent(4, call = false, null, resPut)
    val history = History(List(callGet, returnGet, callPut, returnPut))

    assert(!SimulationUtils.isLinearizable(history))
  }

  /*
  *
  * p1:-|-GET(key)==a-|----
  * p2:-|-PUT(key,a)--|----
  *
  * */
  test("Scenario 2 should be linearizable") {
    val uuidGet = UUID.randomUUID()
    val reqGet = Get("key", uuidGet)
    val resGet = OperationResponse(uuidGet, OpCode.Ok, Some("value"))
    val callGet = ExecutionEvent(1, call = true, reqGet, null)
    val returnGet = ExecutionEvent(2, call = false, null, resGet)

    val uuidPut = UUID.randomUUID()
    val reqPut = Put("key", "value", uuidPut)
    val resPut = OperationResponse(uuidPut, OpCode.Ok, Some("value"))
    val callPut = ExecutionEvent(1, call = true, reqPut, null)
    val returnPut = ExecutionEvent(2, call = false, null, resPut)
    val history = History(List(callGet, callPut, returnGet, returnPut))

    assert(SimulationUtils.isLinearizable(history))
  }

  /*
  *
  * p1:-|-PUT(key, a)-|--------------|-PUT(key, b)-|--
  * p2:------------------|-GET(key)==b-|--------------
  *
  * */
  test("Scenario 3 should be linearizable") {
    val uuidPutA = UUID.randomUUID()
    val reqPutA = Put("key", "a", uuidPutA)
    val resPutA = OperationResponse(uuidPutA, OpCode.Ok, Some("a"))
    val callPutA = ExecutionEvent(1, call = true, reqPutA, null)
    val returnPutA = ExecutionEvent(2, call = false, null, resPutA)

    val uuidGetB = UUID.randomUUID()
    val reqGetB = Get("key", uuidGetB)
    val resGetB = OperationResponse(uuidGetB, OpCode.Ok, Some("b"))
    val callGetB = ExecutionEvent(3, call = true, reqGetB, null)
    val returnGetB = ExecutionEvent(10, call = false, null, resGetB)

    val uuidPutB = UUID.randomUUID()
    val reqPutB = Put("key", "b", uuidPutB)
    val resPutB = OperationResponse(uuidPutB, OpCode.Ok, Some("b"))
    val callPutB = ExecutionEvent(9, call = true, reqPutB, null)
    val returnPutB = ExecutionEvent(13, call = false, null, resPutB)

    val orderedEvents = List(callPutA, returnPutA, callGetB, returnGetB, callPutB, returnPutB)
      .sortWith((a, b) => a.ts < b.ts || (a.ts == b.ts && a.call))
    val history = History(orderedEvents)

    assert(SimulationUtils.isLinearizable(history))
  }

  /*
  *
  * p1:-|-PUT(key, a)-|--------------|-PUT(key, b)-|----------
  * p2:------------------|-GET(key)==b-|----------------------
  * p3:---------------------------------|-CAS(key, a, c)==c)-|
  *
  * */
  test("Scenario 4 should not be linearizable") {
    val uuidPutA = UUID.randomUUID()
    val reqPutA = Put("key", "a", uuidPutA)
    val resPutA = OperationResponse(uuidPutA, OpCode.Ok, Some("a"))
    val callPutA = ExecutionEvent(1, call = true, reqPutA, null)
    val returnPutA = ExecutionEvent(2, call = false, null, resPutA)

    val uuidGetB = UUID.randomUUID()
    val reqGetB = Get("key", uuidGetB)
    val resGetB = OperationResponse(uuidGetB, OpCode.Ok, Some("b"))
    val callGetB = ExecutionEvent(3, call = true, reqGetB, null)
    val returnGetB = ExecutionEvent(10, call = false, null, resGetB)

    val uuidPutB = UUID.randomUUID()
    val reqPutB = Put("key", "b", uuidPutB)
    val resPutB = OperationResponse(uuidPutB, OpCode.Ok, Some("b"))
    val callPutB = ExecutionEvent(9, call = true, reqPutB, null)
    val returnPutB = ExecutionEvent(13, call = false, null, resPutB)

    val uuidCasAC = UUID.randomUUID()
    val reqCasAC = Cas("key", "a", "c", uuidCasAC)
    val resCasAC = OperationResponse(uuidCasAC, OpCode.Ok, Some("c"))
    val callCasAC = ExecutionEvent(11, call = true, reqCasAC, null)
    val returnCasAC = ExecutionEvent(14, call = false, null, resCasAC)

    val orderedEvents = List(callPutA, returnPutA, callGetB, returnGetB, callPutB, returnPutB, callCasAC, returnCasAC)
                            .sortWith((a, b) => a.ts < b.ts || (a.ts == b.ts && a.call))
    val history = History(orderedEvents)

    assert(!SimulationUtils.isLinearizable(history))
  }

  /*
  *
  * p1:-|-PUT(key, a)-|--------------|-PUT(key, b)-|----------
  * p2:------------------|-GET(key)==b-|----------------------
  * p3:---------------------------------|-CAS(key, a, c)==b)-|
  *
  * */
  test("Scenario 5 should be linearizable") {
    val uuidPutA = UUID.randomUUID()
    val reqPutA = Put("key", "a", uuidPutA)
    val resPutA = OperationResponse(uuidPutA, OpCode.Ok, Some("a"))
    val callPutA = ExecutionEvent(1, call = true, reqPutA, null)
    val returnPutA = ExecutionEvent(2, call = false, null, resPutA)

    val uuidGetB = UUID.randomUUID()
    val reqGetB = Get("key", uuidGetB)
    val resGetB = OperationResponse(uuidGetB, OpCode.Ok, Some("b"))
    val callGetB = ExecutionEvent(3, call = true, reqGetB, null)
    val returnGetB = ExecutionEvent(10, call = false, null, resGetB)

    val uuidPutB = UUID.randomUUID()
    val reqPutB = Put("key", "b", uuidPutB)
    val resPutB = OperationResponse(uuidPutB, OpCode.Ok, Some("b"))
    val callPutB = ExecutionEvent(9, call = true, reqPutB, null)
    val returnPutB = ExecutionEvent(13, call = false, null, resPutB)

    val uuidCasAC = UUID.randomUUID()
    val reqCasAC = Cas("key", "a", "c", uuidCasAC)
    val resCasAC = OperationResponse(uuidCasAC, OpCode.Ok, Some("b"))
    val callCasAC = ExecutionEvent(11, call = true, reqCasAC, null)
    val returnCasAC = ExecutionEvent(14, call = false, null, resCasAC)

    val orderedEvents = List(callPutA, returnPutA, callGetB, returnGetB, callPutB, returnPutB, callCasAC, returnCasAC)
      .sortWith((a, b) => a.ts < b.ts || (a.ts == b.ts && a.call))
    val history = History(orderedEvents)

    assert(SimulationUtils.isLinearizable(history))
  }
}
