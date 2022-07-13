package akkaPackage

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.*
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.{Actor, OneForOneStrategy, Props, Scheduler, SupervisorStrategy}
import akka.cluster.ClusterEvent.*
import akka.cluster.typed.{Cluster, Join, Subscribe}
import akka.cluster.{ClusterEvent, MemberStatus}
import akkaPackage.AkkaFlowSystem.{MemberRemovedRequest, MemberUpRequest}
import akka.pattern.retry
import akka.util.Timeout
import akkaPackage.AkkaFlow

import concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.Random

object ClusterWorker extends App:
  sealed trait Command
  case class ReceivedMessage() extends Command
  case class Do(dagNode: DagNode, sender : ActorRef[AkkaFlow.Command]) extends Command

  val serviceKey = ServiceKey[Command]("log-worker")

  def workerBehavior() = Behaviors.setup[Command]{ context =>
    context.system.receptionist ! Receptionist.Register(serviceKey, context.self)
    Behaviors.receiveMessagePartial[Command](msg =>
      msg match
        case Do(dagNode: DagNode, sender: ActorRef[AkkaFlow.Command]) =>
          try
            val parameters = dagNode.doing()
              sender ! AkkaFlow.CommandWithSender(AkkaFlow.DoneFromNode(dagNode.nodeId, dagNode.dagDown, parameters), context.self)
              context.log.info(s"Node ${dagNode.name} in dag ${dagNode.dagName} executed")
            Behaviors.same
          catch
            case error: Error =>
              sender ! AkkaFlow.CommandWithSender(AkkaFlow.ErrorFromNode(dagNode.nodeId, dagNode.dagDown, error), context.self)
              Behaviors.same)
    }

  def behavior(count : Int) : Behavior[ClusterEvent.MemberRemoved] = Behaviors.setup[ClusterEvent.MemberRemoved]{ context =>
    val cluster = Cluster(context.system)
    val main = cluster.selfMember.address.copy(port = Some(25520))
    cluster.subscriptions ! Subscribe(context.self, classOf[ClusterEvent.MemberRemoved])
    cluster.manager ! Join(main)

    Range(0, count).foreach(_ => context.spawnAnonymous(workerBehavior()))
    
    Behaviors.receiveMessagePartial{ msg =>
      msg match
        case ClusterEvent.MemberRemoved(m, _) if m.address == main =>
          Behaviors.stopped
    }
  }

  val sys = ActorSystem(behavior(100), "Main")