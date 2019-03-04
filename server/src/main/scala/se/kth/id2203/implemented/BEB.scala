package se.kth.id2203.implemented;

import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.{ComponentDefinition => _, Port => _, KompicsEvent}
import se.kth.id2203.networking._

import scala.collection.immutable.Set
import scala.collection.mutable.ListBuffer

class BestEffortBroadcast extends Port {
 indication[BEB_Deliver];
 request[BEB_Broadcast];
}

case class BEB_Deliver(source: NetAddress, payload: KompicsEvent) extends KompicsEvent;
case class BEB_Broadcast(payload: KompicsEvent) extends KompicsEvent;

class BasicBroadcast(init: Init[BasicBroadcast]) extends ComponentDefinition {

  //ports
  val beb = provides[BestEffortBroadcast];
  val net = requires[Network];
  val topol = requires[Topology];

  //configuration
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  var topology: Set[NetAddress] = Set.empty;

  //handlers
  beb uponEvent {
    case x: BEB_Broadcast => handle {
         for (p <- topology) {
             trigger( NetMessage(self, p, x) -> net )
         }
    }
  }

  net uponEvent {
    case NetMessage(header, BEB_Broadcast(payload)) => handle {
        trigger( BEB_Deliver(header.src, payload) -> beb )
    }
  }
  
  topol uponEvent {
    case Provide_topology(nodes) => handle {
      topology = nodes;
    }
  }
}