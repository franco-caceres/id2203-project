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
import se.kth.id2203.kvstore.Get
import se.kth.id2203.networking._
import se.kth.id2203.simulation.LinearizabilityScenarios.{intToClientAddress, intToServerAddress}
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

class BroadcastTest extends FlatSpec with Matchers {
  "Scenario 1: Node 192.193.0.1 receives a request and sends it to a node in the correct partition." +
    "All nodes in the correct partitions" should "deliver a client request whose key belongs to their partition" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val scenario = BroadcastScenarios.scenario1(6)
    scenario.simulate(classOf[LauncherComp]);
  }
}


object BroadcastScenarios {
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
  val startGetClientOp = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1));
    StartNode(selfAddr, Init[ScenarioClient](Get("test")), conf);
  }

  def scenario1(servers: Int): JSimulationScenario = {
    val networkSetup = raise(1, setUniformLatencyNetwork()).arrival(constant(0))
    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second))
    val startGetClient = raise(1, startGetClientOp, 1.toN).arrival(constant(0.seconds))

    networkSetup
      .andThen(0.seconds)
      .afterTermination(startCluster)
      .andThen(10.seconds)
      .afterTermination(startGetClient)
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
