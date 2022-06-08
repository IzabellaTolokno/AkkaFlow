package akkaPackage

import akka.event.Logging
import akka.actor.*
import akkaPackage.AkkaFlow.*

import java.time.LocalDateTime
import scala.language.postfixOps
import concurrent.duration.DurationInt
import akka.actor.typed.Dispatchers
import akka.actor.typed.scaladsl.Behaviors

import java.util.concurrent.Callable
import concurrent.ExecutionContext.Implicits.global

object AkkaFlow:
  trait ResultToNode:
    def result: String
    def parameters: Map[String, String]
    def fromId : Int
    def toId : Int

  case class NotDoneToNode(fromId : Int, toId : Int) extends ResultToNode:
    def result = "Not done"
    def parameters = Map[String, String]()

  case class DoneToNode(fromId : Int, toId : Int, parameters: Map[String, String]) extends ResultToNode:
    def result = "Done"

  case class ErrorToNode(fromId : Int, toId : Int, error: Error) extends ResultToNode:
    def result = "Error"
    def parameters = Map[String, String]()


  case class SkippedToNode(fromId : Int, toId : Int) extends ResultToNode:
    def result = "Skip"
    def parameters = Map[String, String]()


  trait ResultFromNode:
    def result: String
    def parameters: Map[Int, Map[String, String]]
    def fromId : Int
    def toId : List[Int]

  case class NotDoneFromNode(fromId : Int, toId : List[Int]) extends ResultFromNode:
    def result = "Not done"
    def parameters = Map[Int, Map[String, String]]()

  case class DoneFromNode(fromId : Int, toId : List[Int], parameters: Map[Int, Map[String, String]]) extends ResultFromNode:
    def result = "Done"

  case class ErrorFromNode(fromId : Int, toId :  List[Int], error: Error) extends ResultFromNode:
    def result = "Error"
    def parameters = Map[Int, Map[String, String]]()


  case class SkippedFromNode(fromId : Int, toId : List[Int]) extends ResultFromNode:
    def result = "Skip"
    def parameters = Map[Int, Map[String, String]]()

  case class Received(fromId : Int, toId : Int)
  case class ReceivedFromRoot()

  case class AskInfo(Id : Int)





class AkkaFlow(val dag: DAG, val system : ActorRef, val storage : ActorRef) extends Actor :
  val log = Logging(context.system, this)

  override def preStart() = {
    log.info(s"Start DAG ${dag.dagName}")
  }

  override val supervisorStrategy = OneForOneStrategy(){
    case _ => SupervisorStrategy.Restart
  }

  override def postRestart(reason: Throwable): Unit =
    log.info(s"Restart DAG ${dag.dagName}")
    storage ! Restart(dag.dagName)
    context.become(start())

  var results: Map[Int, ResultFromNode] = dag.Ids.map(nodeId => {
    context.actorOf(Props(classOf[AkkaFlowNode], dag.node(nodeId)),  dag.intToName(nodeId))
    nodeId -> NotDoneFromNode(nodeId, dag.dagDown(nodeId))
  }).toMap

  var scheduler: Map[(Int, Int), Cancellable] = Map[(Int, Int), Cancellable]()

  def resendMessage(result: ResultFromNode) =
    for nodeId <- result.toId do
      context.child(dag.intToName(nodeId)) match
        case Some(actorRef: ActorRef) =>
          results.getOrElse(nodeId, NotDoneFromNode(result.fromId, result.toId)) match
            case _ : NotDoneFromNode => scheduler = scheduler + ((result.fromId, nodeId) -> context.system.scheduler
              .scheduleAtFixedRate(0 millis, 1000 millis, actorRef, convertMessage(result)(nodeId)))
            case _ =>
        case _ =>

  log.info(s"All nodes of DAG ${dag.dagName} is created")

  def convertMessage(resultFromNode: ResultFromNode) : Int => ResultToNode =
    toId => resultFromNode match {
      case _: DoneFromNode => DoneToNode(resultFromNode.fromId, toId, resultFromNode.parameters.getOrElse(toId, Map[String, String]()))
      case _ : NotDoneFromNode => NotDoneToNode(resultFromNode.fromId, toId)
      case error: ErrorFromNode => ErrorToNode(resultFromNode.fromId, toId, error.error)
      case _: SkippedFromNode => SkippedToNode(resultFromNode.fromId, toId)
    }

  def start() : Receive =
    case Results(description : Map[Int, ResultFromNode]) => {
      if dag.Ids.isEmpty then
        log.info("Empty dag")
        storage ! Done(dag.dagName)
        context.stop(self)
      results = dag.Ids.map(nodeId => {
        description.getOrElse(nodeId, NotDoneFromNode(nodeId, dag.dagDown(nodeId))) match
          case result : NotDoneFromNode =>
            nodeId -> result
          case result : ResultFromNode =>
            resendMessage(result)
            nodeId -> result
      }).toMap
      context.become(work())
    }

  def work() : Receive =
    case result : ResultFromNode =>
      storage ! NewResult(dag.dagName, result)
      results = results + (result.fromId -> result)
      if results.forall((_, result) => result match
        case _ : NotDoneFromNode => false
        case _ => true) then
        storage ! Done(dag.dagName)
        log.info(s"DAG ${dag.dagName} finish")
        context.stop(self)
      else
        resendMessage(result)
      sender() ! ReceivedFromRoot()

    case Received(fromId, toId) => scheduler((fromId, toId)).cancel()

    case AskInfo(id) => results.foreach((_, result) => if result.toId.contains(id) then sender() ! convertMessage(result)(id))

    case message =>
      log.info(s"Wrong message $message")

  override def receive: Receive = work()

class AkkaFlowNode(val dagNode: DagNode) extends Actor :
  val log = Logging(context.system, this)
  var results: Map[Int, ResultToNode] = Map[Int, ResultToNode]()
  var state = false
  var scheduler : List[Cancellable] = List()

  for node <- dagNode.dagUp do results = results + (node -> NotDoneToNode(node, dagNode.nodeId))

  def check(): Unit =
    if dagNode.condition.condition(results) && !state then
      log.info(s"Condition for ${dagNode.name} in DAG ${dagNode.dagName} is completed")
      state = true
      try
        val parameters =  dagNode.doing()
        scheduler = List(context.system.scheduler.scheduleAtFixedRate(0 millis, 1000 millis,
          context.parent, DoneFromNode(dagNode.nodeId, dagNode.dagDown, parameters)))
      catch
        case error: Error => scheduler = List(context.system.scheduler.scheduleAtFixedRate(0 millis, 1000 millis,
          context.parent, ErrorFromNode(dagNode.nodeId, dagNode.dagDown, error)))


    if dagNode.condition.conditionSkipped(results) && !state then
      log.info(s"Condition for ${dagNode.name} in DAG ${dagNode.dagName} is failed")
      state = true
      scheduler = List(context.system.scheduler.scheduleAtFixedRate(0 millis, 1000 millis,
        context.parent,  SkippedFromNode(dagNode.nodeId, dagNode.dagDown)))

  check()

  override def receive =
    case result : ResultToNode => {
      if !state then
        results = results + (result.fromId -> result)
        check()
      context.parent ! Received(result.fromId, result.toId)
    }
    case _ : ReceivedFromRoot => {
      scheduler.foreach(cancellable => cancellable.cancel())
      context.stop(self)
    }
    case message => log.info(s"Wrong message $message")


