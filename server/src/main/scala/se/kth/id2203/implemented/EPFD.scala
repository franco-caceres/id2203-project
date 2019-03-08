package se.kth.id2203.implemented

import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.KompicsEvent
import se.kth.id2203.networking._

case class Suspect(process: NetAddress) extends KompicsEvent
case class Restore(process: NetAddress) extends KompicsEvent

class EventuallyPerfectFailureDetector extends Port {
  indication[Suspect]
  indication[Restore]
}

case class CheckTimeoutFD(timeout: ScheduleTimeout) extends Timeout(timeout)
case class HeartbeatReply(seq: Int) extends KompicsEvent
case class HeartbeatRequest(seq: Int) extends KompicsEvent

class EPFD(epfdInit: Init[EPFD]) extends ComponentDefinition {
  val timer = requires[Timer]
  val net = requires[Network]
  val epfd = provides[EventuallyPerfectFailureDetector]
  val topo = requires[Topology]

  //configuration parameters
  val self = cfg.getValue[NetAddress]("id2203.project.address")
  var topology: Set[NetAddress] = Set(self)
  val delta = cfg.getValue[Long]("id2203.project.epfd.delay")

  //mutable state
  var period = cfg.getValue[Long]("id2203.project.epfd.delay")
  var alive: Set[NetAddress] = Set(self)
  var suspected = Set[NetAddress]()
  var seqnum = 0

  def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(delay)
    scheduledTimeout.setTimeoutEvent(CheckTimeoutFD(scheduledTimeout))
    trigger(scheduledTimeout -> timer)
  }

  timer uponEvent {
    case CheckTimeoutFD(_) => handle {
      if (alive.intersect(suspected).nonEmpty) {
        period += delta
      }
      seqnum = seqnum + 1
      for (p <- topology) {
        if (!alive.contains(p) && !suspected.contains(p)) {
          suspected += p
          trigger(Suspect(p) -> epfd)
        } else if (alive.contains(p) && suspected.contains(p)) {
          suspected = suspected - p
          trigger(Restore(p) -> epfd)
        }
        trigger(NetMessage(self, p, HeartbeatRequest(seqnum)) -> net)
      }
      alive = Set[NetAddress]()
      startTimer(period)
    }
  }

  net uponEvent {
    case NetMessage(header, HeartbeatRequest(seq)) => handle {
      trigger(NetMessage(self, header.src, HeartbeatReply(seq)) -> net)
    }
    case NetMessage(header, HeartbeatReply(seq)) => handle {
      if(seq == seqnum || suspected.contains(header.src)) {
        alive += header.src
      }
    }
  }

  topo uponEvent {
    case FullTopology(nodes) => handle {
      topology = nodes
      alive = nodes
      trigger(Suspect(self) -> epfd)
      startTimer(period)
    }
  }
}
