package akkaPackage

import akka.actor.typed.scaladsl.*
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props, SupervisorStrategy}
import akka.cluster.ClusterEvent.*
import akka.cluster.{Cluster, ClusterEvent, MemberStatus}


class ClusterWorker extends Actor {
  val cluster = Cluster(context.system)
  cluster.subscribe(self, classOf[ClusterEvent.MemberRemoved])
  val main = cluster.selfAddress.copy(port = Some(25520))
  cluster.join(main)
  def receive = {
    case ClusterEvent.MemberRemoved(m, _) =>
      if (m.address == main) context.stop(self)
  }
}