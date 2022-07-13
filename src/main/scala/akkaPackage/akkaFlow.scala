package akkaPackage

import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.event.Logging
import akka.actor.{Actor, ActorLogging, Address}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, PostStop, PreRestart, Scheduler, SupervisorStrategy}
import akka.cluster.typed.{Cluster, Join, Subscribe}
import akka.cluster.ClusterEvent
import akkaPackage.*
import akka.pattern.{ask, retry}
import akka.remote.RemoteScope
import akka.util.Timeout
import akkaPackage.AkkaFlowSystem.{CheckExecuted, SystemStatus}

import java.time.Duration
import concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Random, Success}

object AkkaFlow:
  sealed trait Command

  trait ResultFromNode extends Command:
    def result: String
    def parameters: Map[Int, Map[String, String]]
    def fromId : Int
    def toId : List[Int]

  case class NotDoneFromNode(fromId : Int, toId : List[Int]) extends ResultFromNode:
    def result = "Not done"
    def parameters: Map[Int, Map[String, String]] = Map[Int, Map[String, String]]()

  case class DoneFromNode(fromId : Int, toId : List[Int], parameters: Map[Int, Map[String, String]]) extends ResultFromNode:
    def result = "Done"

  case class ErrorFromNode(fromId : Int, toId :  List[Int], error: Error) extends ResultFromNode:
    def result = "Error"
    def parameters: Map[Int, Map[String, String]] = Map[Int, Map[String, String]]()


  case class SkippedFromNode(fromId : Int, toId : List[Int]) extends ResultFromNode:
    def result = "Skip"
    def parameters: Map[Int, Map[String, String]] = Map[Int, Map[String, String]]()

  case class Received(fromId : Int, toId : Int)
  case class ReceivedFromRoot()

  case class AskInfo(Id : Int) extends Command

  case class ReceivedMessage() extends Command
  case class UnreceivedMessage() extends Command

  case class Do(id : Int) extends Command

  case class ListingResponse(listing: Receptionist.Listing) extends Command

  case class Results(list: Map[Int, ResultFromNode]) extends Command

  case class CommandWithSender[T](message: Command, sender : ActorRef[T]) extends Command

  def behavior(dag: DAG, system : ActorRef[AkkaFlowSystem.Command], storage : ActorRef[Storage.Command]) : Behavior[Command] = Behaviors.supervise(Behaviors.setup[Command]{ context =>
    implicit val timeout : Timeout = Timeout(1.seconds)
    implicit val scheduler : Scheduler = context.system.scheduler
    import akka.actor.typed.Dispatchers
    context.log.info(s"Start DAG ${dag.dagName}")

    val listingResponseAdapter = context.messageAdapter[Receptionist.Listing](ListingResponse.apply)
    context.system.receptionist ! Receptionist.Subscribe(ClusterWorker.serviceKey, listingResponseAdapter)

    var results: Map[Int, ResultFromNode] = Map()
    var children: Map[Int, ActorRef[AkkaFlowNode.Command]] = Map()

    def resendMessage(result: ResultFromNode): Unit =
      for nodeId <- result.toId do
        context.child(dag.intToName(nodeId)) match
          case Some(actorRef: ActorRef[AkkaFlowNode.Command]) =>
            results.getOrElse(nodeId, NotDoneFromNode(result.fromId, result.toId)) match
              case _ : NotDoneFromNode =>
                actorRef ! convertMessage(result)(nodeId)
                context.ask(actorRef, _ => convertMessage(result)(nodeId)){
                  case Success(SystemStatus(ReceivedMessage())) =>
                    ReceivedMessage()
                  case _ =>
                    actorRef ! convertMessage(result)(nodeId)
                    UnreceivedMessage()
                }
              case _ =>
          case _ =>

    def convertMessage(resultFromNode: ResultFromNode) : Int => AkkaFlowNode.Command =
      toId => resultFromNode match {
        case _: DoneFromNode => AkkaFlowNode.DoneToNode(resultFromNode.fromId, toId, resultFromNode.parameters.getOrElse(toId, Map[String, String]()))
        case _ : NotDoneFromNode => AkkaFlowNode.NotDoneToNode(resultFromNode.fromId, toId)
        case error: ErrorFromNode => AkkaFlowNode.ErrorToNode(resultFromNode.fromId, toId, error.error)
        case _: SkippedFromNode => AkkaFlowNode.SkippedToNode(resultFromNode.fromId, toId)
      }

    def start() : Behavior[Command] = Behaviors.receiveMessage[Command]  { msg =>
      msg match
        case Results(description: Map[Int, ResultFromNode]) =>
          if dag.Ids.isEmpty then
            context.log.info("Empty dag")
            storage ! Storage.Done(dag.dagName)
            Behaviors.stopped
          else
            results = dag.Ids.map(nodeId => {
              description.getOrElse(nodeId, NotDoneFromNode(nodeId, dag.dagDown(nodeId))) match
                case result: NotDoneFromNode =>
                  nodeId -> result
                case result: ResultFromNode =>
                  resendMessage(result)
                  nodeId -> result
            }).toMap
            active(Set())
    }

    def waitListing(): Behavior[Command]  = Behaviors.receiveMessagePartial[Command] { msg =>
      msg match
        case ListingResponse(ClusterWorker.serviceKey.Listing(listings)) if listings.nonEmpty =>
          results = dag.Ids.map(nodeId => {
            val actorRef = context.spawn(AkkaFlowNode.behavior(dag.node(nodeId), context.self),  dag.intToName(nodeId))
            children = children + (nodeId -> actorRef)
            nodeId -> NotDoneFromNode(nodeId, dag.dagDown(nodeId))
          }).toMap

          context.log.info(s"All nodes of DAG ${dag.dagName} is created")
          active(listings)
        case ListingResponse(ClusterWorker.serviceKey.Listing(listings)) if listings.isEmpty =>
          Behaviors.same
        case _ =>
          context.log.info(msg.toString)
          Behaviors.same
    }

    def active(addresses: Set[ActorRef[ClusterWorker.Command]]) : Behavior[Command] = Behaviors.receiveMessage[Command]  {  msg =>
      msg match
        case CommandWithSender(result: SkippedFromNode, sender : ActorRef[AkkaFlowNode.Command]) =>
          storage ! Storage.NewResult(dag.dagName, result)
          results = results + (result.fromId -> result)
          if results.forall((_, result) => result match
            case _: NotDoneFromNode => false
            case _ => true) then
            storage ! Storage.Done(dag.dagName)
            context.log.info(s"DAG ${dag.dagName} finish")
            sender ! AkkaFlowNode.ReceivedMessage()
            context.stop(context.self)
            Behaviors.same
          else
            resendMessage(result)
            sender ! AkkaFlowNode.ReceivedMessage()
            Behaviors.same
        case CommandWithSender(result: ResultFromNode, sender : ActorRef[ClusterWorker.Command]) =>
          storage ! Storage.NewResult(dag.dagName, result)
          results = results + (result.fromId -> result)
          if results.forall((_, result) => result match
            case _: NotDoneFromNode => false
            case _ => true) then
            storage ! Storage.Done(dag.dagName)
            context.log.info(s"DAG ${dag.dagName} finish")
            sender ! ClusterWorker.ReceivedMessage()
            Behaviors.stopped
          else
            resendMessage(result)
            sender ! ClusterWorker.ReceivedMessage()
            Behaviors.same

        case CommandWithSender(AskInfo(id), sender : ActorRef[AkkaFlowNode.Command]) =>
          results.foreach((_, result) => if result.toId.contains(id) then sender ! convertMessage(result)(id))
          Behaviors.same

        case ReceivedMessage() => Behaviors.same
        case UnreceivedMessage() => Behaviors.same

        case ClusterWorker.serviceKey.Listing(listings) => active(listings)

        case CommandWithSender(Do(id), sender : ActorRef[AkkaFlowNode.Command]) =>
          val actorRef = Random.shuffle(addresses.toList).head
          actorRef ! ClusterWorker.Do(dag.node(id), context.self)
          sender ! AkkaFlowNode.ReceivedMessage()
          Behaviors.same

        case ListingResponse(ClusterWorker.serviceKey.Listing(listings)) if listings.nonEmpty =>
          active(listings)

        case ListingResponse(ClusterWorker.serviceKey.Listing(listings)) if listings.isEmpty =>
          waitListing()
    }.receiveSignal {
      case (_, signal) if signal == PreRestart =>
        context.log.info(s"Restart DAG ${dag.dagName}")
        storage ! Storage.Restart(dag.dagName)
        start()
      case (_, signal) if signal == PostStop =>
        start()
    }
    waitListing()
  }).onFailure[Error](SupervisorStrategy.restart)



object AkkaFlowNode:
  sealed trait Command

  sealed trait ResultToNode extends Command:
    def result: String
    def parameters: Map[String, String]
    def fromId : Int
    def toId : Int

  case class NotDoneToNode(fromId : Int, toId : Int) extends ResultToNode:
    def result = "Not done"
    def parameters: Map[String, String] = Map[String, String]()

  case class DoneToNode(fromId : Int, toId : Int, parameters: Map[String, String]) extends ResultToNode:
    def result = "Done"

  case class ErrorToNode(fromId : Int, toId : Int, error: Error) extends ResultToNode:
    def result = "Error"
    def parameters: Map[String, String] = Map[String, String]()


  case class SkippedToNode(fromId : Int, toId : Int) extends ResultToNode:
    def result = "Skip"
    def parameters: Map[String, String] = Map[String, String]()

  case class ReceivedMessage() extends Command

  def behavior(dagNode: DagNode, parent : ActorRef[AkkaFlow.Command]) : Behavior[AkkaFlowNode.Command] = Behaviors.setup[AkkaFlowNode.Command]{ context =>
    implicit val timeout : Timeout = Timeout(1.seconds)
    import concurrent.ExecutionContext.Implicits.global

    var results: Map[Int, ResultToNode] = Map[Int, ResultToNode]()
    var state = false

    for node <- dagNode.dagUp do results = results + (node -> NotDoneToNode(node, dagNode.nodeId))

    implicit val scheduler : Scheduler = context.system.scheduler

    def check(): Unit =
      if dagNode.condition.condition(results) && !state then
        context.log.info(s"Condition for ${dagNode.name} in DAG ${dagNode.dagName} is completed")
        state = true
        parent ! AkkaFlow.CommandWithSender(AkkaFlow.Do(dagNode.nodeId), context.self)

      if dagNode.condition.conditionSkipped(results) && !state then
        context.log.info(s"Condition for ${dagNode.name} in DAG ${dagNode.dagName} is failed")
        state = true
        parent ! AkkaFlow.CommandWithSender(AkkaFlow.SkippedFromNode(dagNode.nodeId, dagNode.dagDown), context.self)

    check()

    Behaviors.receiveMessage {
      case result: ResultToNode =>
        if !state then
          results = results + (result.fromId -> result)
          check()
        parent ! AkkaFlow.ReceivedMessage()
        Behaviors.same
      case ReceivedMessage() => Behaviors.stopped
      case message =>
        context.log.info(s"Wrong message $message")
        Behaviors.same
    }
  }
