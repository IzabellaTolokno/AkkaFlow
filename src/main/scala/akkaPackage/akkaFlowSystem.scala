package akkaPackage

import akka.actor.*
import akka.cluster.{Cluster, ClusterEvent}
import akkaPackage.*
import akkaPackage.AkkaFlow.ResultFromNode
import akkaPackage.akkaFlowSystem.Check

import concurrent.duration.DurationInt
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.io.Source.fromFile
import scala.language.postfixOps

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.Behaviors

import akka.actor.typed.scaladsl._
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus

object akkaFlowSystem:
  case class Check(name : String)


class akkaFlowSystem extends Actor with Stash:
  import context.dispatcher

  override val supervisorStrategy = OneForOneStrategy(){
    case _ => SupervisorStrategy.Restart
  }
  val storage = context.actorOf(Props(classOf[Storage], Map[String, Map[Int, ResultFromNode]](), Map[String, Storage.Status]()))

  implicit val timeout : Timeout = Timeout(500.millis)

  override def receive = active(Vector())
  
  context.parent ! ClusterMain.Ask(self)

  def active(addresses: Vector[Address]) : Receive =
    case dag : DAG => context.actorOf(Props(classOf[AkkaFlow], dag, self, storage, addresses), dag.dagName)
    case Check(name : String) => (storage ? Storage.AskStatus(name)).pipeTo(sender())
    case ClusterMain.Add(address) =>
      context.become(active(addresses :+ address))
      context.children.foreach(child => child ! ClusterMain.Add(address))
    case ClusterMain.Remove(address) =>
      val next = addresses filterNot (_ == address)
      context.become(active(next))
      context.children.foreach(child => child ! ClusterMain.Remove(address))

