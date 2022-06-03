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




class AkkaFlow(val dag: DAG, var system : ActorRef) extends Actor :
  val log = Logging(context.system, this)

  override def preStart() = {
    log.info("Start Root")
  }

  if dag.Ids.isEmpty then
    log.info("Empty dag")
    system ! "Done"
    context.stop(self)

  var Done: Map[Int, Boolean] = dag.Ids.foldLeft(Map[Int, Boolean]())((map, node) => map + (node -> false))
  val dagActorRef: Map[Int, ActorRef] = dag.Ids.foldLeft(Map[Int, ActorRef]())((map, nodeId) =>
    map + (nodeId -> context.actorOf(Props(classOf[AkkaFlowNode], dag.node(nodeId), self))))


  def getMessegeFrom(fromId : Int, toId : List[Int], newResult: Int => ResultToNode): Unit =
    Done = Done + (fromId -> true)
    if Done.forall((_, bool) => bool) then
      system ! "Done"
      log.info("Finish")
      context.stop(self)
    else
      for node <- toId do dagActorRef(node) ! newResult(node)

  def receive = {
    case done : DoneFromNode =>
      getMessegeFrom(done.fromId, done.toId, toId => DoneToNode(done.fromId, toId,
        done.parameters.getOrElse(toId, Map[String, String]())))

    case notDone : NotDoneFromNode =>
      getMessegeFrom(notDone.fromId, notDone.toId, toId => NotDoneToNode(notDone.fromId, toId))

    case error: ErrorFromNode =>
      getMessegeFrom(error.fromId, error.toId, toId => ErrorToNode(error.fromId, toId, error.error))

    case skipped: SkippedFromNode =>
      getMessegeFrom(skipped.fromId, skipped.toId, toId => SkippedToNode(skipped.fromId, toId))

    case message =>
      log.info(s"Wrong message $message")
  }


class AkkaFlowNode(val dagNode: DagNode, val root : ActorRef) extends Actor :

  val log = Logging(context.system, this)
  var results: Map[Int, ResultToNode] = Map[Int, ResultToNode]()
  var state = false

  for node <- dagNode.dagUp do results = results + (node -> NotDoneToNode(node, dagNode.nodeId))

  def check(): Unit =
    if dagNode.condition.condition(results) && !state then
      state = true
      root ! DoneFromNode(dagNode.nodeId, dagNode.dagDown, dagNode.doing())

    if dagNode.condition.conditionSkipped(results) && !state then
      state = true
      root ! SkippedFromNode(dagNode.nodeId, dagNode.dagDown)
  check()

  override def receive =
    case result : ResultToNode => {
      results = results + (result.fromId -> result)
      check()
    }


