package akkaPackage

import akka.actor.*
import akka.cluster.ClusterEvent
import akkaPackage.*
import akkaPackage.AkkaFlow.{Command, ResultFromNode}
import akkaPackage.AkkaFlowSystem.Check

import java.time.Duration
import concurrent.duration.DurationInt
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.io.Source.fromFile
import scala.language.postfixOps
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.*
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.cluster.ClusterEvent.*
import akka.cluster.MemberStatus
import akka.cluster.typed.{Cluster, Join, Subscribe}
import akkaPackage.Storage.Command

import scala.util.{Failure, Success}

object AkkaFlowSystem:
  sealed trait Command
  case class MemberUpRequest(event: ClusterEvent.MemberUp) extends Command
  case class MemberRemovedRequest(event: ClusterEvent.MemberRemoved) extends Command
  case class Check(name: String, sender : ActorRef[Storage.Status]) extends Command
  case class SystemStatus(status: Storage.Status) extends Command
  case class DagRequest(dag : DAG) extends Command
  case class CheckExecuted() extends Command

  val SystemServiceKey: ServiceKey[Command] = ServiceKey[Command]("System")

  def behavior(): Behavior[Command] = Behaviors.setup[Command] { context =>
    implicit val timeout: Timeout = Timeout(500.millis)

    val cluster = Cluster(context.system)
    cluster.manager ! Join(cluster.selfMember.address)

    var children = List[ActorRef[AkkaFlow.Command]]()
    val storage = context.spawn(Storage.behavior(), "Storage")


    context.system.receptionist ! Receptionist.Register(SystemServiceKey, context.self)

    def active(): Behavior[Command] = Behaviors.receiveMessagePartial[Command] { message =>
      message match
        case DagRequest(dag: DAG) =>
          children = children ++ List(context.spawn(AkkaFlow.behavior(dag, context.self, storage), dag.dagName))
          Behaviors.same
        case Check(name: String, sender: ActorRef[Storage.Status])=>
          context.ask(storage, ref => Storage.StorageMessageWithSender(Storage.AskStatus(name), ref)){
            case Success(SystemStatus(message : Storage.Status)) =>
              sender ! message
              CheckExecuted()
            case Failure(_) =>
              sender ! Storage.Unknown(name)
              CheckExecuted()
          }
          Behaviors.same
        case CheckExecuted() => Behaviors.same
    }
    active()
  }
