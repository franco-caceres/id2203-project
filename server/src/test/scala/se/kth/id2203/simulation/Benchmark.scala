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

class Benchmark extends FlatSpec with Matchers {
  "Regular sequence Paxos" should "be slower" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val scenario = BenchmarkScenarios.scenario1(10)
    val now = System.currentTimeMillis()
    scenario.simulate(classOf[LauncherComp]);
    println("Regular sequence Paxos took " + (System.currentTimeMillis() - now) + "ms.")
  }

  "Sequence Paxos with time leases" should "be faster" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val scenario = BenchmarkScenarios.scenario2(10)
    val now = System.currentTimeMillis()
    scenario.simulate(classOf[LauncherComp]);
    println("Sequence Paxos with time leases took " + (System.currentTimeMillis() - now) + "ms.")
  }
}

object BenchmarkScenarios {
  import Distributions._
  implicit val random: Random = JSimulationScenario.getRandom
  val setUniformLatencyNetwork: () => adaptor.Operation[ChangeNetworkModelEvent] = () => Op.apply((_: Unit) => ChangeNetwork(NetworkModels.withUniformRandomDelay(35, 35)))
  val startServerOp = Op { (self: Integer) =>
    val selfAddr = intToServerAddress(self)
    val conf = if (isBootstrap(self)) {
      // don't put this at the bootstrap server, or it will act as a bootstrap client
      Map("id2203.project.address" -> selfAddr,
        "id2203.project.bootThreshold" -> 10,
        "id2203.project.replicationDegree" -> 10,
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
        "id2203.project.bootThreshold" -> 10,
        "id2203.project.replicationDegree" -> 10,
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
    val startGetClients = raise(255, startGetClientOp, 1.toN).arrival(constant(100.milliseconds))
    val startPutClients = raise(10, startPutClientOp, 2.toN, 1.toN).arrival(constant(1.second))

    networkSetup
      .andThen(0.seconds)
      .afterTermination(startCluster)
      .andThen(30.seconds)
      .afterTermination(startGetClients)
      .inParallel(startPutClients)
      .andThen(3.seconds)
      .afterTermination(Terminate)
  }

  def scenario2(servers: Int): JSimulationScenario = {
    val networkSetup = raise(1, setUniformLatencyNetwork()).arrival(constant(0))
    val startClusterTimeLease = raise(servers, startServerTimeLeaseOp, 1.toN).arrival(constant(1.second))
    val startGetClients = raise(255, startGetClientOp, 1.toN).arrival(constant(100.milliseconds))
    val startPutClients = raise(10, startPutClientOp, 2.toN, 1.toN).arrival(constant(1.second))

    networkSetup
      .andThen(0.seconds)
      .afterTermination(startClusterTimeLease)
      .andThen(30.seconds)
      .afterTermination(startGetClients)
      .inParallel(startPutClients)
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
