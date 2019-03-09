/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.simulation

import java.net.{InetAddress, UnknownHostException}
import java.util.Random

import org.scalatest._
import se.kth.id2203.ParentComponent
import se.kth.id2203.kvstore._
import se.kth.id2203.networking._
import se.sics.kompics.network.Address
import se.sics.kompics.simulator.adaptor.Operation1
import se.sics.kompics.simulator.events.system.{ChangeNetworkModelEvent, KillNodeEvent}
import se.sics.kompics.simulator.network.PartitionMapper
import se.sics.kompics.simulator.network.identifier.{Identifier, IdentifierExtractor}
import se.sics.kompics.simulator.network.impl.NetworkModels
import se.sics.kompics.simulator.run.LauncherComp
import se.sics.kompics.simulator.util.GlobalView
import se.sics.kompics.simulator.{adaptor, SimulationScenario => JSimulationScenario}
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator._
import se.sics.kompics.network.{Network, Address, Header, Msg, Transport};

import scala.concurrent.duration._

class LinearizabilityTest extends FlatSpec with Matchers {
  "Parallel GET, PUT, and CAS" should "be linearizable" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val simpleBootScenario = LinearizabilityScenarios.scenario1(6)
    SimulationResult += ("history" -> SimulationUtils.serialize(SerializedHistory()))
    simpleBootScenario.simulate(classOf[LauncherComp]);
    val history = SimulationUtils.deserialize[SerializedHistory](SimulationResult.get[String]("history").get).deserialize
    println(history)
    SimulationUtils.isLinearizable(history) should be (true)
  }

  "GET after a failure still works and" should "be linearizable" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val simpleBootScenario = LinearizabilityScenarios.scenario2(6)
    SimulationResult += ("history" -> SimulationUtils.serialize(SerializedHistory()))
    simpleBootScenario.simulate(classOf[LauncherComp]);
    val history = SimulationUtils.deserialize[SerializedHistory](SimulationResult.get[String]("history").get).deserialize
    println(history)
    SimulationUtils.isLinearizable(history) should be (true)
  }

  "GET after 2 failures (no quorum attainable)" should "not complete but still be linearizable" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val simpleBootScenario = LinearizabilityScenarios.scenario3(6)
    SimulationResult += ("history" -> SimulationUtils.serialize(SerializedHistory()))
    simpleBootScenario.simulate(classOf[LauncherComp]);
    val history = SimulationUtils.deserialize[SerializedHistory](SimulationResult.get[String]("history").get).deserialize
    println(history)
    SimulationUtils.isComplete(history) should be (false)
    SimulationUtils.isLinearizable(history) should be (true)
  }

  "Time lease with partition" should "allow reads but no modifications while leader is separated from the other replicas," +
    "Meanwhile the isolated set of replicas (2) have to wait until the lease expires to achieve their own quorum." in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val simpleBootScenario = LinearizabilityScenarios.scenario4(6)
    SimulationResult += ("history" -> SimulationUtils.serialize(SerializedHistory()))
    simpleBootScenario.simulate(classOf[LauncherComp]);
    val history = SimulationUtils.deserialize[SerializedHistory](SimulationResult.get[String]("history").get).deserialize
    println(history)
    SimulationUtils.isLinearizable(history) should be (true)
  }
}

case class ExecutionEvent(ts: Long, isCall: Boolean, op: Operation = null, res: OperationResponse = null) extends Serializable
case class History(events: List[ExecutionEvent] = List.empty)
case class SerializedHistory(var serializedEvents: String = "") extends  Serializable {
  def deserialize: History = {
    if(serializedEvents.isEmpty) {
      return History(List.empty)
    }
    val serializedEventList: Array[String] = serializedEvents.split("#").drop(1)
    val events = serializedEventList
      .map(x => SimulationUtils.deserialize[ExecutionEvent](x))
      .sortWith((a, b) => a.ts < b.ts || (a.ts == b.ts && a.isCall))
      .toList
    History(events)
  }
}


object LinearizabilityScenarios {
  import Distributions._
  implicit val random: Random = JSimulationScenario.getRandom
  val setUniformLatencyNetwork: () => adaptor.Operation[ChangeNetworkModelEvent] = () => Op.apply((_: Unit) => ChangeNetwork(NetworkModels.withUniformRandomDelay(33, 100)))
  val startServerOp = Op { (self: Integer) =>
    val selfAddr = intToServerAddress(self)
    val conf = if (isBootstrap(self)) {
      // don't put this at the bootstrap server, or it will act as a bootstrap client
      Map("id2203.project.address" -> selfAddr,
          "id2203.project.bootThreshold" -> 6,
          "id2203.project.replicationDegree" -> 3,
          "id2203.project.minKey" -> Int.MinValue,
          "id2203.project.maxKey" -> Int.MaxValue,
          "id2203.project.useTimeLease" -> false,
          "id2203.project.ble.delay" -> 100,
          "id2203.project.epfd.delay" -> 100)
    } else {
      Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(1),
        "id2203.project.minKey" -> Int.MinValue,
        "id2203.project.maxKey" -> Int.MaxValue,
        "id2203.project.useTimeLease" -> false,
        "id2203.project.ble.delay" -> 100,
        "id2203.project.epfd.delay" -> 100)
    };
    StartNode(selfAddr, Init.none[ParentComponent], conf);
  }
  val startServerTimeLeaseOp = Op { (self: Integer) =>
    val selfAddr = intToServerAddress(self)
    val conf = if (isBootstrap(self)) {
      // don't put this at the bootstrap server, or it will act as a bootstrap client
      Map("id2203.project.address" -> selfAddr,
        "id2203.project.bootThreshold" -> 6,
        "id2203.project.replicationDegree" -> 3,
        "id2203.project.minKey" -> Int.MinValue,
        "id2203.project.maxKey" -> Int.MaxValue,
        "id2203.project.useTimeLease" -> true,
        "id2203.project.ble.delay" -> 40,
        "id2203.project.epfd.delay" -> 40)
    } else Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1),
      "id2203.project.minKey" -> Int.MinValue,
      "id2203.project.maxKey" -> Int.MaxValue,
      "id2203.project.leaseDuration" -> 10000,
      "id2203.project.clock.error" -> 1,
      "id2203.project.useTimeLease" -> true,
      "id2203.project.ble.delay" -> 40,
      "id2203.project.epfd.delay" -> 40);
    StartNode(selfAddr, Init.none[ParentComponent], conf);
  }
  val startClientOp = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1));
    StartNode(selfAddr, Init.none[ScenarioClient], conf);
  }
  val startGetClientOp = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1));
    StartNode(selfAddr, Init[ScenarioClient](Get("test")), conf);
  }
  val startGetClientWithTargetOp = Op { (self: Integer, target: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(target));
    StartNode(selfAddr, Init[ScenarioClient](Get("test")), conf);
  }
  val startPutClientOp = Op { (self: Integer, value: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1));
    StartNode(selfAddr, Init[ScenarioClient](Put("test", value.toString)), conf);
  }
  val startPutClientWithTargetOp = Op { (self: Integer, value: Integer, target: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(target));
    StartNode(selfAddr, Init[ScenarioClient](Put("test", value.toString)), conf);
  }
  val startCasClientOp = Op { (self: Integer, compareValue: Integer, setValue: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1));
    StartNode(selfAddr, Init[ScenarioClient](Cas("test", compareValue.toString, setValue.toString)), conf);
  }
  val killServerOp = new Operation1[KillNodeEvent, Integer]() {
    override def generate(self: Integer): KillNodeEvent = new KillNodeEvent() {
      var selfAdr: Address = intToServerAddress(self)
      override

      def getNodeAddress = selfAdr

      override

      def toString: String = "KILL<" + selfAdr.toString + ">"
    }
  }

  def scenario1(servers: Int): JSimulationScenario = {
    val networkSetup = raise(1, setUniformLatencyNetwork()).arrival(constant(0))
    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second))
    val startGetClient = raise(1, startGetClientOp, 1.toN).arrival(constant(1.second))
    val startPutClient = raise(1, startPutClientOp, 2.toN, 1.toN).arrival(constant(1.second))
    val startCasClient = raise(1, startCasClientOp, 3.toN, 1.toN, 2.toN).arrival(constant(1.second))

    networkSetup
      .andThen(0.seconds)
      .afterTermination(startCluster)
      .andThen(10.seconds)
      .afterTermination(startCasClient)
      .inParallel(startGetClient)
      .inParallel(startPutClient)
      .andThen(5.seconds)
      .afterTermination(Terminate)
  }

  def scenario2(servers: Int): JSimulationScenario = {
    val networkSetup = raise(1, setUniformLatencyNetwork()).arrival(constant(0))
    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second))
    val startGetClient = raise(1, startGetClientOp, 1.toN).arrival(constant(1.second))
    val startPutClient = raise(1, startPutClientOp, 2.toN, 1.toN).arrival(constant(1.second))
    val killServer = raise(1, killServerOp, 4.toN).arrival(constant(0.seconds))

    networkSetup
      .andThen(0.seconds)
      .afterTermination(startCluster)
      .andThen(10.seconds)
      .afterTermination(startPutClient)
      .andThen(3.seconds)
      .afterTermination(killServer)
      .andThen(5.seconds)
      .afterTermination(startGetClient)
      .andThen(5.seconds)
      .afterTermination(Terminate)
  }

  def scenario3(servers: Int): JSimulationScenario = {
    val networkSetup = raise(1, setUniformLatencyNetwork()).arrival(constant(0))
    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second))
    val startGetClient = raise(1, startGetClientOp, 1.toN).arrival(constant(1.second))
    val startPutClient = raise(1, startPutClientOp, 2.toN, 1.toN).arrival(constant(1.second))
    val killServer1 = raise(1, killServerOp, 4.toN).arrival(constant(0.seconds))
    val killServer2 = raise(1, killServerOp, 5.toN).arrival(constant(0.seconds))

    networkSetup
      .andThen(0.seconds)
      .afterTermination(startCluster)
      .andThen(10.seconds)
      .afterTermination(startPutClient)
      .andThen(3.seconds)
      .afterTermination(killServer1)
      .inParallel(killServer2)
      .andThen(3.seconds)
      .afterTermination(startGetClient)
      .andThen(5.seconds)
      .afterTermination(Terminate)
  }

  def scenario4(servers: Int): JSimulationScenario = {
    val uniformLatency: () => adaptor.Operation[ChangeNetworkModelEvent] = () => Op.apply((_: Unit) => ChangeNetwork(NetworkModels.withConstantDelay(10)))
    val separate4And5Setup: () => adaptor.Operation[ChangeNetworkModelEvent] =
      () => Op.apply(
        (_: Unit) =>
          ChangeNetwork(
            NetworkModels.withPartitionedModel(
              (adr: Address) => (_: Int) => adr.getIp.toString.split('.').last.toInt,
              NetworkModels.withConstantDelay(10),
              (nodeId: Identifier) => {
                // isolate servers whose IP end in .4 and .5
                if (Seq(4, 5).contains(nodeId.partition(1))) {
                  2
                } else {
                  1
                }
              }
            )
          )
      )
    val networkSetup = raise(1, uniformLatency()).arrival(constant(0))
    val separate4And5 = raise(1, separate4And5Setup()).arrival(constant(0))
    val startClusterTimeLease = raise(servers, startServerTimeLeaseOp, 1.toN).arrival(constant(1.second))
    val startGetClient = raise(1, startGetClientOp, 1.toN).arrival(constant(0.second))
    val startPut1Client = raise(1, startPutClientOp, 2.toN, 1.toN).arrival(constant(0.second))
    val startPut2Client = raise(1, startPutClientOp, 3.toN, 2.toN).arrival(constant(0.second))
    val startPut3ClientInPartitionWith4And5 = raise(1, startPutClientWithTargetOp, 4.toN, 3.toN, 5.toN).arrival(constant(0.second))
    val startPut4Client = raise(1, startPutClientOp, 4.toN, 4.toN).arrival(constant(0.second))

    networkSetup
      .andThen(0.seconds)
      .afterTermination(startClusterTimeLease)
      .andThen(10.seconds) // 6 is leader
      .afterTermination(startPut1Client) // put(test, 1) using leader
      .andThen(1.seconds)
      .afterTermination(separate4And5) // partition network: all - {4, 5} and {4,5}
      .andThen(1.seconds)
      .afterTermination(startGetClient) // get(test) from isolated leader (server 6) holding lease
      .inParallel(startPut2Client) // call to server 6 (leader with (LEADER, ACCEPT)) with put(test, 2), which is never completed due to lost connectivity
      .inParallel(startPut3ClientInPartitionWith4And5) // call to server 5 (isolated along with 4) from client in same partition with put(test, 2), which is held by new leader 5 with state (LEADER, PREPARE), which holds it until it receives lease
      .andThen(10.seconds)
      .afterTermination(networkSetup) // connectivity is restored
      .andThen(3.seconds)
      .afterTermination(startGetClient)
      .andThen(3.seconds)
      .afterTermination(startPut4Client)
      .inParallel(startGetClient)
      .andThen(3.seconds)
      .afterTermination(Terminate)
  }

  private def intToServerAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.0." + i), 45678)
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex)
    }
  }

  private def intToClientAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.1." + i), 45678)
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex)
    }
  }

  private def isBootstrap(self: Int): Boolean = self == 1
}
