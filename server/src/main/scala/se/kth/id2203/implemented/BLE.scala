package se.kth.id2203.implemented;

import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.{KompicsEvent, Start}
import se.kth.id2203.networking._

import scala.collection.mutable;

class BallotLeaderElection extends Port {
    indication[BLE_Leader];
}
case class BLE_Leader(leader: NetAddress, ballot: Long) extends KompicsEvent;

//Primitives that are used in the implementation
object Primitives {  
  private val ballotOne = 0x0100000000l;
  
  def ballotFromNAddress(n: Int, adr: NetAddress): Long = {
    val nBytes = com.google.common.primitives.Ints.toByteArray(n);
    val addrBytes = com.google.common.primitives.Ints.toByteArray(adr.hashCode());
    val bytes = nBytes ++ addrBytes;
    val r = com.google.common.primitives.Longs.fromByteArray(bytes);
    assert(r > 0); // should not produce negative numbers!
    return r;
  }
  
  def incrementBallotBy(ballot: Long, inc: Int): Long = {
    ballot + inc.toLong * ballotOne
  }
  
  private def incrementBallot(ballot: Long): Long = {
    ballot + ballotOne
  }
}

case class CheckTimeout(timeout: ScheduleTimeout) extends Timeout(timeout);
case class HeartbeatReq(round: Long, highestBallot: Long) extends KompicsEvent;
case class HeartbeatResp(round: Long, ballot: Long) extends KompicsEvent;

class GossipLeaderElection(init: Init[GossipLeaderElection]) extends ComponentDefinition {

  val ble = provides[BallotLeaderElection];
  val net = requires[Network];
  val timer = requires[Timer];
  val topol = requires[Topology];

  val self = cfg.getValue[NetAddress]("id2203.project.address");
  var topology: Set[NetAddress] = Set.empty;
  val delta = 1;      //the 1 second initial delta is selected groundlessly
  val majority = (topology.size / 2) + 1;

  private var period = cfg.getValue[Long]("ble.simulation.delay");
  private var ballots = mutable.Map.empty[NetAddress, Long];

  private var round = 0l;
  private var ballot = Primitives.ballotFromNAddress(0, self);

  private var leader: Option[(Long, NetAddress)] = None;
  private var highestBallot: Long = ballot;

  private def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  private def checkLeader() {
    var ballotsAndSelf = ballots + ( self -> ballot );
    var (topProcess, topBallot) = ballotsAndSelf.maxBy{
      case (key, value) => value
    }
    if( topBallot < highestBallot ){
      while( ballot <= highestBallot ){
        ballot = Primitives.incrementBallotBy(ballot, 1);
      }
      leader = None;
    }
    else{
      if( !leader.isDefined || ((topBallot, topProcess) != leader.get) ){
        highestBallot = topBallot;
        leader = Some((topBallot, topProcess));
        trigger( BLE_Leader(topProcess, topBallot) -> ble );
      }
    }
  }

  ctrl uponEvent {
    case _: Start => handle {
      // FCG not implemented yet so no need to start it
      // startTimer(period);
    }
  }

  timer uponEvent {
    case CheckTimeout(_) => handle {
      if( ballots.size + 1 >= majority ){
        checkLeader();
      }
      ballots.clear;
      round += 1;
      for(p <- topology) {
        if( p != self  ){
            trigger( NetMessage(self, p, HeartbeatReq(round, highestBallot)) -> net )
        }
      }
      startTimer(period);
    }
  }

  net uponEvent {
    case NetMessage( header, HeartbeatReq(r, hb) ) => handle {
      if( hb > highestBallot ){
        highestBallot = hb;
      }
      trigger( NetMessage(self, header.src, HeartbeatResp(r, ballot)) -> net );
    }
    case NetMessage( header, HeartbeatResp(r, b) ) => handle {
      if( r == round ){
        ballots += (header.src -> b);
      }
      else{
        period += delta;
      }
    }
  }
  
  topol uponEvent {
    case Provide_topology(nodes) => handle {
      topology = nodes;
    }
  }
}

