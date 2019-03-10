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
package se.kth.id2203

;

import se.kth.id2203.bootstrapping._
import se.kth.id2203.kvstore.KVService
import se.kth.id2203.networking.NetAddress
import se.kth.id2203.overlay._
import se.sics.kompics.network.Network
import se.sics.kompics.sl._
import se.sics.kompics.timer.Timer
import se.kth.id2203.implemented._;

class ParentComponent extends ComponentDefinition {
  //******* Ports ******
  val net = requires[Network];
  val timer = requires[Timer];

  //******* Children ******
  val overlay = create(classOf[VSOverlayManager], Init.NONE);
  val kv = create(classOf[KVService], Init.NONE);
  val boot = cfg.readValue[NetAddress]("id2203.project.bootstrap-address") match {
    case Some(_) => create(classOf[BootstrapClient], Init.NONE); // start in client mode
    case None => create(classOf[BootstrapServer], Init.NONE); // start in server mode
  }
  val beb = create(classOf[BasicBroadcast], Init[BasicBroadcast]());
  val rb = create(classOf[EagerReliableBroadcast], Init[EagerReliableBroadcast]());
  val ble = create(classOf[GossipLeaderElection], Init[GossipLeaderElection]());
  val sc = cfg.readValue[Boolean]("id2203.project.useTimeLease") match {
    case Some(true) => create(classOf[SequencePaxosTimeLease], Init[SequencePaxosTimeLease]())
    case _ => create(classOf[SequencePaxos], Init[SequencePaxos]())
  }
  val epfd = create(classOf[EPFD], Init[EPFD]())

  {
    // Bootstrapping
    connect[Timer](timer -> boot);
    connect[Network](net -> boot);
    // Overlay
    connect(Bootstrapping)(boot -> overlay);
    connect[Network](net -> overlay);
    connect[EventuallyPerfectFailureDetector](epfd -> overlay)
    // KV
    connect[Network](net -> kv);
    // broadcast
    connect[Network](net -> beb);
    connect[BestEffortBroadcast](beb -> rb);
    connect[ReliableBroadcast](rb -> kv);
    connect[Topology](overlay -> beb);
    // leader election
    connect[Network](net -> ble);
    connect[Timer](timer -> ble);
    connect[Topology](overlay -> ble);
    // paxos
    connect[Network](net -> sc);
    connect[Topology](overlay -> sc);
    connect[BallotLeaderElection](ble -> sc);
    connect[SequenceConsensus](sc -> kv);
    cfg.readValue[Boolean]("id2203.project.useTimeLease") match {
      case Some(true) => connect[Timer](timer -> sc);
      case _ =>
    }
    // epfd
    connect[Topology](overlay -> epfd)
    connect[Network](net -> epfd)
    connect[Timer](timer -> epfd)
  }
}
