package akkaPackage

import akka.event.Logging
import akka.actor.*
import akka.cluster.{Cluster, ClusterEvent}
import akkaPackage.AkkaFlow.*
import akkaPackage.Storage.*
import akka.pattern.{ask, retry}
import akka.util.Timeout

import concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext


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

  val ReceivedMessage: String = "Received"





class AkkaFlow(val dag: DAG, val system : ActorRef, val storage : ActorRef, addresses: Vector[Address]) extends Actor with ActorLogging:
  implicit val executionContext: ExecutionContext = context.dispatcher
  implicit val timeout : Timeout = Timeout(1.seconds)
  implicit val scheduler: akka.actor.Scheduler = context.system.scheduler
  

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

  override def postStop(): Unit = {
    super.postStop()
  }


  var results: Map[Int, ResultFromNode] = dag.Ids.map(nodeId => {
    context.actorOf(Props(classOf[AkkaFlowNode], dag.node(nodeId)),  dag.intToName(nodeId))
    nodeId -> NotDoneFromNode(nodeId, dag.dagDown(nodeId))
  }).toMap


  def resendMessage(result: ResultFromNode) =
    for nodeId <- result.toId do
      context.child(dag.intToName(nodeId)) match
        case Some(actorRef: ActorRef) =>
          results.getOrElse(nodeId, NotDoneFromNode(result.fromId, result.toId)) match
            case _ : NotDoneFromNode =>
              akka.pattern.retry(() => (actorRef ? convertMessage(result)(nodeId))
                .map(str => if str == ReceivedMessage then str else Error())
                .recover(error => {
                  results.getOrElse(nodeId, NotDoneFromNode(result.fromId, result.toId)) match
                    case _ : NotDoneFromNode => error
                    case _ => ReceivedMessage
                }), attempts = 10, 100.millis)
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
      context.become(active(Vector()))
    }

  def active(addresses: Vector[Address]) : Receive = {
    case result: ResultFromNode =>
      storage ! NewResult(dag.dagName, result)
      results = results + (result.fromId -> result)
      if results.forall((_, result) => result match
        case _: NotDoneFromNode => false
        case _ => true) then
        storage ! Done(dag.dagName)
        log.info(s"DAG ${dag.dagName} finish")
        sender() ! ReceivedMessage
        context.stop(self)
      else
        resendMessage(result)
        sender() ! ReceivedMessage

    case AskInfo(id) => results.foreach((_, result) => if result.toId.contains(id) then sender() ! convertMessage(result)(id))

    case message if message == ReceivedMessage =>

    case ClusterMain.Add(address) =>
      context.become(active(addresses :+ address))
    case ClusterMain.Remove(address) =>
      val next = addresses filterNot (_ == address)
      context.become(active(next))
      ???

    case message => log.info(s"Wrong message $message")
  }

  override def receive: Receive = active(Vector())

class AkkaFlowNode(val dagNode: DagNode) extends Actor with ActorLogging:
  import context.dispatcher
  implicit val timeout : Timeout = Timeout(1.seconds)
  implicit val scheduler: akka.actor.Scheduler = context.system.scheduler

  var results: Map[Int, ResultToNode] = Map[Int, ResultToNode]()
  var state = false

  for node <- dagNode.dagUp do results = results + (node -> NotDoneToNode(node, dagNode.nodeId))

  override def postStop(): Unit = {
    super.postStop()
  }

  def check(): Unit =
    if dagNode.condition.condition(results) && !state then
      log.info(s"Condition for ${dagNode.name} in DAG ${dagNode.dagName} is completed")
      state = true
      try
        val parameters =  dagNode.doing()
        akka.pattern.retry(() => (context.parent ? DoneFromNode(dagNode.nodeId, dagNode.dagDown, parameters))
          .map(str => if str == ReceivedMessage then str else Error()), attempts = 10, 1.millis)
      catch
        case error: Error =>
          akka.pattern.retry(() => (context.parent ? ErrorFromNode(dagNode.nodeId, dagNode.dagDown, error))
            .map(str => if str == ReceivedMessage then str else Error()), attempts = 10, 1000.millis)


    if dagNode.condition.conditionSkipped(results) && !state then
      log.info(s"Condition for ${dagNode.name} in DAG ${dagNode.dagName} is failed")
      state = true
      akka.pattern.retry(() => (context.parent ? SkippedFromNode(dagNode.nodeId, dagNode.dagDown))
        .map(str => if str == ReceivedMessage then str else Error()), attempts = 10, 1000.millis)

  check()

  override def receive = {
    case result: ResultToNode => {
      if !state then
        results = results + (result.fromId -> result)
        check()
      context.parent ! ReceivedMessage
    }
    case message: String if message == ReceivedMessage => context.stop(self)
    case message => log.info(s"Wrong message $message")
  }


