package akkaPackage

import akka.event.Logging
import akka.actor.*
import akkaPackage.AkkaFlow._

import java.time.LocalDateTime
import scala.language.postfixOps

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




class AkkaFlow(val dag: DAG, var system : ActorRef) extends Actor :
  val log = Logging(context.system, this)

  override def preStart() = {
    log.info("Start Root")
  }

  var dagActorRef = Map[Int, ActorRef]()
  var Done = Map[Int, Boolean]()

  for node <- dag.nodeSet() do
    Done = Done + (node -> false)
    dagActorRef = dagActorRef + (node -> context.actorOf(Props(classOf[AkkaFlowNode], DagNode(dag.up(node),
      dag.down(node), node, dag.condition(node), dag.doing(node)), self)))

  def receive = {
    case done : DoneFromNode =>
      log.info(s"Root get done message from ${done.fromId}")
      Done = Done + (done.fromId -> true)
      if Done.forall((_, bool) => bool) then
        system ! "Done"
        log.info("Finish")
        context.stop(self)
      for node <- done.toId do dagActorRef(node) ! DoneToNode(done.fromId, node,
        done.parameters.getOrElse(node, Map[String, String]()))

    case notDone : NotDoneFromNode =>
      Done = Done + (notDone.fromId -> true)
      if Done.forall((_, bool) => bool) then
        system ! "Done"
        log.info("Finish")
        context.stop(self)
      for node <- notDone.toId do dagActorRef(node) ! NotDoneToNode(notDone.fromId, node)

    case error: ErrorFromNode =>
      Done = Done + (error.fromId -> true)
      if Done.forall((_, bool) => bool) then
        system ! "Done"
        log.info("Finish")
        context.stop(self)
      for node <- error.toId do dagActorRef(node) ! ErrorToNode(error.fromId, node, error.error)

    case message =>
      log.info(s"Wrong message $message")
  }


class AkkaFlowNode(val dagNode: DagNode, val root : ActorRef) extends Actor :

  val log = Logging(context.system, this)


  var results: Map[Int, ResultToNode] = Map[Int, ResultToNode]()
  var parameters: Map[Int, Map[String, String]] = Map[Int, Map[String, String]]()
  var state = false

  for node <- dagNode.dagUp do results = results + (node -> NotDoneToNode(node, dagNode.id))

  for node <- dagNode.dagDown do {
    parameters = parameters + (node -> Map[String, String]())
  }

  def check(): Unit =
    if dagNode.condition(results) && !state then
      state = true
      parameters = dagNode.doing()

      root ! DoneFromNode(dagNode.id, dagNode.dagDown, parameters)
  check()

  override def receive =
    case result : ResultToNode => {
      results = results + (result.fromId -> result)
      check()
    }


