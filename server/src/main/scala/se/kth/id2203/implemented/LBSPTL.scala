package se.kth.id2203.implemented;

import se.kth.id2203.implemented
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.KompicsEvent
import se.sics.kompics.network.Network
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}

import scala.collection.mutable

case class Nack(n: Long) extends KompicsEvent
case class ReplyToNackTimeout(p: NetAddress, nL: Long, ld: Int, na: Long, timeout: ScheduleTimeout) extends Timeout(timeout)

class SequencePaxosTimeLease(init: Init[SequencePaxosTimeLease]) extends ComponentDefinition {
  def suffix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
    s.drop(l)
  }

  def prefix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
    s.take(l)
  }

  import Role._
  import State._

  val sc = provides[SequenceConsensus];
  val ble = requires[BallotLeaderElection];
  val net = requires[Network];
  val topo = requires[Topology]
  val timer = requires[Timer];

  val self = cfg.getValue[NetAddress]("id2203.project.address");
  var pi: Set[NetAddress] = Set(self)
  var others: Set[NetAddress] = Set.empty
  var majority: Int = (pi.size / 2) + 1

  var state: (implemented.Role.Value, implemented.State.Value) = (FOLLOWER, UNKNOWN);
  var nL = 0l;
  var nProm = 0l;
  var leader: Option[NetAddress] = None;
  var na = 0l;
  var va = List.empty[RSM_Command];
  var ld = 0;
  // leader state
  var propCmds = List.empty[RSM_Command];
  val las = mutable.Map.empty[NetAddress, Int];
  val lds = mutable.Map.empty[NetAddress, Int];
  var lc = 0;
  val acks = mutable.Map.empty[NetAddress, (Long, List[RSM_Command])];

  // lease
  val leaseDuration = cfg.getValue[Long]("id2203.project.leaseDuration")
  val clockError = cfg.getValue[Long]("id2203.project.clock.error")
  var tprom = 0l
  var tl = 0l
  var hasGivenAnyLease = false
  var nacks = 0

  def clockTime: Long = {
    System.currentTimeMillis()
  }

  def canGiveLease(n: Long): Boolean = {
    if(n <= nProm) {
      return false
    }
    if(!hasGivenAnyLease) {
      true
    } else {
      (clockTime - tprom) > leaseDuration*(1000 + clockError)/1000.0
    }
  }

  def canReplyWithLocalState(c: RSM_Command): Boolean = {
    if(!c.isRead) {
      return false
    }
    if(state != (LEADER, ACCEPT)) {
      return false
    }
    (clockTime - tl) < leaseDuration*(1000 - clockError)/1000.0
  }

  topo uponEvent {
    case PartitionTopology(nodes: Set[NetAddress]) => handle {
      pi = nodes
      others = pi - self
      majority = (pi.size / 2) + 1
    }
  }

  ble uponEvent {
    case BLE_Leader(l, n) => handle {
      if(n > nL) {
        leader = Some(l);
        nL = n;
        if(self == leader.get && nL > nProm) {
          println(clockTime/1000)
          state = (LEADER, PREPARE);
          propCmds = List.empty;
          las.clear();
          pi.foreach(p => las(p) = 0);
          lds.clear();
          acks.clear();
          lc = 0;
          tl = clockTime
          nacks = 0
          others.foreach(p => {
            trigger(NetMessage(self, p, Prepare(nL, ld, na)) -> net)
          });
          acks += self -> (na, suffix(va, ld));
          lds += self -> ld;
          nProm = nL;
        } else {
          state = (FOLLOWER, state._2);
        }
      }
    }
  }

  net uponEvent {
    case NetMessage(header, Prepare(np, ldp, n)) => handle {
      val p = header.src
      if(canGiveLease(np)) {
        hasGivenAnyLease = true
        tprom = clockTime
        nProm = np;
        state = (FOLLOWER, PREPARE);
        var sfx = List.empty[RSM_Command];
        if(na >= n) {
          sfx = suffix(va, ldp);
        }
        trigger(NetMessage(self, p, Promise(np, na, sfx, ld)) -> net);
      } else {
        // if the only reason a promise wasn't given is the lease violation, then ask to try again later
        if(np > nProm && (clockTime - tprom) <= leaseDuration*(1000 + clockError)/1000.0) {
          trigger(NetMessage(self, p, Nack(np)) -> net)
        }
      }
    }
    case NetMessage(header, Promise(n, nap, sfxa, lda)) => handle {
      val a = header.src
      if ((n == nL) && (state == (LEADER, PREPARE))) {
        acks += a -> (nap, sfxa);
        lds += a -> lda;
        if(acks.size >= majority) {
          val k = acks.map(x => x._2._1).max;
          val sfx = acks.filter(x => x._2._1 == k).head._2._2;
          va = prefix(va,  ld) ++ sfx ++ propCmds;
          las(self) = va.size;
          propCmds = List.empty[RSM_Command];
          state = (LEADER, ACCEPT);
          others.foreach(p => {
            if(lds.exists(x => x._1 == p)) {
              val sfxp = suffix(va, lds(p));
              trigger(NetMessage(self, p, AcceptSync(nL, sfxp, lds(p))) -> net);
            }
          });
        }
      } else if ((n == nL) && (state == (LEADER, ACCEPT))) {
        lds += a -> lda;
        val sfx = suffix(va, lds(a));
        trigger(NetMessage(self, a, AcceptSync(nL, sfx, lds(a))) -> net);
        if(lc != 0) {
          trigger(NetMessage(self, a, Decide(ld, nL)) -> net);
        }
      }
    }
    case NetMessage(header, Nack(n)) => handle {
      val p = header.src
      if(n == nL && state == (LEADER, PREPARE)) {
        val scheduledTimeout = new ScheduleTimeout(500)
        scheduledTimeout.setTimeoutEvent(ReplyToNackTimeout(p, nL, ld, na, scheduledTimeout))
        trigger(scheduledTimeout -> timer)
      }
    }
    case NetMessage(header, AcceptSync(xL, sfx, ldp)) => handle {
      val p = header.src
      if ((nProm == xL) && (state == (FOLLOWER, PREPARE))) {
        na = xL;
        va = prefix(va, ldp) ++ sfx;
        trigger(NetMessage(self, p, Accepted(xL, va.size)) -> net);
        state = (FOLLOWER, ACCEPT);
      }
    }
    case NetMessage(header, Accept(xL, c)) => handle {
      val p = header.src
      if ((nProm == xL) && (state == (FOLLOWER, ACCEPT))) {
        va = va ++ List(c);
        trigger(NetMessage(self, p, Accepted(xL, va.size)) -> net);
      }
    }
    case NetMessage(header, Decide(l, xL)) => handle {
      val p = header.src
      if(nProm == xL) {
        while(ld < l) {
          trigger(SC_Decide(va(ld)) -> sc);
          ld += 1;
        }
      }
    }
    case NetMessage(header, Accepted(n, m)) => handle {
      val a = header.src
      if ((n == nL) && (state == (LEADER, ACCEPT))) {
        las += a -> m;
        if(lc < m && las.count(x => x._2 >= m) >= majority) {
          lc = m;
          pi.foreach(p => {
            if(lds.exists(x => x._1 == p)) {
              trigger(NetMessage(self, p, Decide(lc, nL)) -> net);
            }
          });
        }
      }
    }
  }

  timer uponEvent {
    case ReplyToNackTimeout(p, _nL, _ld, _na, _) => handle {
      trigger(NetMessage(self, p, Prepare(_nL, _ld, _na)) -> net)
    }
  }

  sc uponEvent {
    case SC_Propose(c) => handle {
      if(canReplyWithLocalState(c)) {
        trigger(SC_Decide(c) -> sc)
      } else {
        if (state == (LEADER, PREPARE)) {
          propCmds = propCmds ++ List(c);
        } else if (state == (LEADER, ACCEPT)) {
          va = va ++ List(c);
          las(self) = las(self) + 1;
          others.foreach(p => {
            if(lds.exists(x => x._1 == p)) {
              trigger(NetMessage(self, p, Accept(nL, c)) -> net);
            }
          });
        }
      }
    }
  }
}
