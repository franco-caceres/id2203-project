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
import se.kth.id2203.networking._
import se.sics.kompics.network.Address
import se.sics.kompics.simulator.adaptor.Operation1
import se.sics.kompics.simulator.events.system.{ChangeNetworkModelEvent, KillNodeEvent}
import se.sics.kompics.simulator.network.identifier.Identifier
import se.sics.kompics.simulator.network.impl.NetworkModels
import se.sics.kompics.simulator.run.LauncherComp
import se.sics.kompics.simulator.{adaptor, SimulationScenario => JSimulationScenario}
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator._

import scala.concurrent.duration._

class EPFDTest extends FlatSpec with Matchers {
  "Scenario 1: No node" should "be suspected under normal network conditions" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val scenario = EPFDScenarios.scenario1(6)
    scenario.simulate(classOf[LauncherComp]);
  }

  "Scenario 2: Node  192.193.0.6" should "be suspected to have failed by all other nodes" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val scenario = EPFDScenarios.scenario2(6)
    scenario.simulate(classOf[LauncherComp]);
  }

  "Scenario 3: Node  192.193.0.6" should "be suspected to have failed by all other nodes at first, but then restored after the network partition is resolved" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val scenario = EPFDScenarios.scenario3(6)
    scenario.simulate(classOf[LauncherComp]);
  }
}


object EPFDScenarios {
  import Distributions._
  implicit val random: Random = JSimulationScenario.getRandom
  val setUniformLatencyNetwork: () => adaptor.Operation[ChangeNetworkModelEvent] = () => Op.apply((_: Unit) => ChangeNetwork(NetworkModels.withUniformRandomDelay(35, 35)))
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

    networkSetup
      .andThen(0.seconds)
      .afterTermination(startCluster)
      .andThen(10.seconds)
      .afterTermination(Terminate)
  }

  def scenario2(servers: Int): JSimulationScenario = {
    val networkSetup = raise(1, setUniformLatencyNetwork()).arrival(constant(0))
    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second))
    val killServer = raise(1, killServerOp, 6.toN).arrival(constant(0.seconds))

    networkSetup
      .andThen(0.seconds)
      .afterTermination(startCluster)
      .andThen(10.seconds)
      .afterTermination(killServer)
      .andThen(5.seconds)
      .afterTermination(Terminate)
  }

  def scenario3(servers: Int): JSimulationScenario = {
    val uniformLatency: () => adaptor.Operation[ChangeNetworkModelEvent] = () => Op.apply((_: Unit) => ChangeNetwork(NetworkModels.withConstantDelay(35)))
    val separate6Setup: () => adaptor.Operation[ChangeNetworkModelEvent] =
      () => Op.apply(
        (_: Unit) =>
          ChangeNetwork(
            NetworkModels.withPartitionedModel(
              (adr: Address) => (_: Int) => adr.getIp.toString.split('.').last.toInt,
              NetworkModels.withConstantDelay(10),
              (nodeId: Identifier) => {
                // isolate servers whose IP end in .4 and .5
                if (nodeId.partition(1) == 6) {
                  2
                } else {
                  1
                }
              }
            )
          )
      )
    val networkSetup = raise(1, uniformLatency()).arrival(constant(0))
    val separate4And5 = raise(1, separate6Setup()).arrival(constant(0))
    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second))

    networkSetup
      .andThen(0.seconds)
      .afterTermination(startCluster)
      .andThen(10.seconds)
      .afterTermination(separate4And5) // partition network: all - {6} and {6}
      .andThen(1.seconds)
      .afterTermination(networkSetup) // connectivity is restored
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
