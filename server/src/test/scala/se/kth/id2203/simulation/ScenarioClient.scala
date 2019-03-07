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

;

import java.util.UUID

import se.kth.id2203.kvstore._
import se.kth.id2203.networking._
import se.kth.id2203.overlay.RouteMsg
import se.sics.kompics.Start
import se.sics.kompics.network.Network
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator.SimulationResult
import se.sics.kompics.timer.Timer

import scala.collection.mutable;

class ScenarioClient(init: Init[ScenarioClient]) extends ComponentDefinition {
  //******* Ports ******
  val net = requires[Network];
  val timer = requires[Timer];

  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  val server = cfg.getValue[NetAddress]("id2203.project.bootstrap-address");
  private val pending = mutable.Map.empty[UUID, String];

  val op = init match {
    case Init(o: Operation) => o
  }

  //******* Handlers ******
  ctrl uponEvent {
    case _: Start => handle {
      val routeMsg = RouteMsg(op.key, op)
      trigger(NetMessage(self, server, routeMsg) -> net)
      pending += (op.id -> op.key)
      val currentEvent = ExecutionEvent(System.currentTimeMillis(), call = true, op)
      val history = SimulationUtils.deserialize[SerializedHistory](SimulationResult.get[String]("history").get)
      history.serializedEvents = history.serializedEvents + "#" + SimulationUtils.serialize(currentEvent)
      SimulationResult += ("history" -> SimulationUtils.serialize(history))
    }
  }

  net uponEvent {
    case NetMessage(header, or@OperationResponse(id, status, value)) => handle {
      logger.debug(s"Got OpResponse: $or");
      pending.remove(id) match {
        case Some(key) => {
          val currentEvent = ExecutionEvent(System.currentTimeMillis(), call = false, op = null, or)
          val history = SimulationUtils.deserialize[SerializedHistory](SimulationResult.get[String]("history").get)
          history.serializedEvents = history.serializedEvents + "#" + SimulationUtils.serialize(currentEvent)
          SimulationResult += ("history" -> SimulationUtils.serialize(history))
        }
        case None => logger.warn("ID $id was not pending! Ignoring response.");
      }
    }
  }
}
