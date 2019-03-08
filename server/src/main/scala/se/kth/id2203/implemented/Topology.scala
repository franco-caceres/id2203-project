package se.kth.id2203.implemented;

import se.kth.id2203.networking._
import se.sics.kompics.sl._
import se.sics.kompics.{KompicsEvent}

//this port is used to provide the system's current topology
class Topology extends Port {
    indication[PartitionTopology]
    indication[FullTopology]
}

case class PartitionTopology(nodes: Set[NetAddress]) extends KompicsEvent;
case class FullTopology(nodes: Set[NetAddress]) extends KompicsEvent;