package akkaPackage

import akka.actor.{Actor, ActorRef, Address}
import akka.cluster.{Cluster, ClusterEvent}

object ClusterMain:
  case class Ask(actorRef: ActorRef)

  sealed trait ClusterInfo:
    val address: Address
  case class Remove(address: Address) extends ClusterInfo
  case class Add(address: Address) extends ClusterInfo

class ClusterMain extends Actor{
  val cluster = Cluster(context.system)
  cluster.subscribe(self, classOf[ClusterEvent.MemberRemoved])
  cluster.subscribe(self, classOf[ClusterEvent.MemberUp])
  cluster.join(cluster.selfAddress)

  val test = cluster.selfAddress.copy(port = Some(10))
  
  override def receive : Receive = active(Vector())
  
  def active(addresses: Vector[Address]): Receive = ({
    case ClusterEvent.MemberUp(member) if member.address != cluster.selfAddress && member.address != test=>
      context.become(active(addresses :+ member.address))
      context.children.foreach(child => child ! ClusterMain.Add(member.address))
    case ClusterEvent.MemberRemoved(member, _) =>
      val next = addresses filterNot (_ == member.address)
      context.become(active(next))
      context.children.foreach(child => child ! ClusterMain.Remove(member.address))
    case ClusterMain.Ask(actorRef) => actorRef ! addresses
      
  }: Receive)


}
