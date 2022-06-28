package akkaPackage

import akkaPackage.AkkaFlow.{DoneToNode, ErrorToNode, NotDoneToNode, ResultToNode}
import akkaPackage.DAG.{Condition, conditionByName}
import com.sun.xml.internal.ws.encoding.soap.DeserializationException

import scala.io.Source.fromFile
import spray.json.{JsArray, *}

import scala.util.Random

object DAG:
  sealed trait Condition:
    val name : String
    val condition : Map[Int, ResultToNode] => Boolean
    val conditionSkipped : Map[Int, ResultToNode] => Boolean

  case class AllSuccess(dependency : List[Int]) extends Condition:
    override val name = "all_success"
    override val condition = m => dependency.forall(x => m(x) match
      case DoneToNode(_, _, _) => true
      case _ => false)
    override val conditionSkipped = m => dependency.exists(x => m(x) match
      case DoneToNode(_, _, _) => false
      case NotDoneToNode(_, _) => false
      case _ => true)

  case class OneSuccess(dependency : List[Int]) extends Condition:
    override val name = "one_success"
    override val condition = m => dependency.exists(x => m(x) match
      case DoneToNode(_, _, _) => true
      case _ => false)
    override val conditionSkipped = m => dependency.forall(x => m(x) match
      case DoneToNode(_, _, _) => false
      case NotDoneToNode(_, _) => false
      case _ => true)

  case class AllFailed(dependency : List[Int]) extends Condition:
    override val name = "all_failed"
    override val condition = m => dependency.forall(x => m(x) match
      case ErrorToNode(_, _, _) => true
      case _ => false)
    override val conditionSkipped = m => dependency.exists(x => m(x) match
      case ErrorToNode(_, _, _) => false
      case NotDoneToNode(_, _) => false
      case _ => true)

  case class OneFailed(dependency : List[Int]) extends Condition:
    override val name = "one_failed"
    override val condition = m => dependency.exists(x => m(x) match
      case ErrorToNode(_, _, _) => true
      case _ => false)
    override val conditionSkipped = m => dependency.forall(x => m(x) match
      case ErrorToNode(_, _, _) => false
      case NotDoneToNode(_, _) => false
      case _ => true)

  def conditionByName(name : String, dependency: List[Int]) =
    name match
      case "all_success" => AllSuccess(dependency)
      case "one_success" => OneSuccess(dependency)
      case "all_failed" => AllFailed(dependency)
      case "one_failed" => OneFailed(dependency)
      case _ => throw Error("Wrong condition")

  def generateDag(quantityNodes : Int): DAG =
    Range(0, quantityNodes).foldLeft(DAG(Iterator.continually(Random.nextPrintableChar)
      .filter(_.isLetter)
      .take(15)
      .mkString)) ((dag, i) => {
      val nDependency = if i > 2 then math.min(Random.nextInt(i - 1) + 1, 50) else 0
      val dependency = Range(0, nDependency).map(_ => Random.nextInt(i)).toSet
      dag.add("Node_" + i.toString + "_dag_" + dag.dagName, i, dependency.toList,
        "all_success",
        () => Map[Int, Map[String, String]]())
    })





case class DAG(dagName : String, Ids: Set[Int] = Set[Int](),
               dagUp : Map[Int, List[Int]] = Map[Int, List[Int]]().withDefaultValue(List[Int]()),
               dagDown : Map[Int, List[Int]] = Map[Int, List[Int]]().withDefaultValue(List[Int]()),
               nameToInt : Map[String, Int] = Map[String, Int](),
               intToName : Map[Int, String]= Map[Int, String](),
               intToDoing : Map[Int, () => Map[Int, Map[String, String]]] = Map[Int, () => Map[Int, Map[String, String]]](),
               conditions : Map[Int, Condition] = Map[Int, Condition]()):
  def add(name: String, nodeId: Int, dependency: List[Int], condition: String,
          doing : () => Map[Int, Map[String, String]]) : DAG =
    DAG(dagName, Ids + nodeId, dagUp + (nodeId -> dependency), dependency.foldLeft(dagDown)((dagDown, node) =>
      dagDown + (node -> (List(nodeId) ++ dagDown.getOrElse(node,  List[Int]())))),
      nameToInt + (name -> nodeId), intToName + (nodeId -> name), intToDoing + (nodeId -> doing),
      conditions + (nodeId -> DAG.conditionByName(condition, dependency)))
    
  def node(nodeId : Int) = DagNode(dagName, intToName(nodeId), dagUp(nodeId),
    dagDown(nodeId), nodeId, conditions(nodeId), intToDoing(nodeId))


  def isEmpty(nodeId : Int) = dagDown(nodeId).isEmpty


class DagNode(val dagName : String, val name : String, val dagUp : List[Int], val dagDown : List[Int], val nodeId : Int, val condition: Condition,
              val doing : () => Map[Int, Map[String, String]])

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit object ColorJsonFormat extends RootJsonFormat[DAG] {
    def write(dag : DAG) = {
      JsObject("dag name" -> JsString(dag.dagName),
        "nodes" -> JsArray(dag.Ids.map(nodeId => {
        JsObject(
          "name" -> JsString(dag.intToName(nodeId)),
          "id" -> JsNumber(nodeId),
          "dependency" -> JsArray(dag.dagUp(nodeId).map(x => JsNumber(x)).toVector),
          "condition" -> JsString(dag.conditions(nodeId).name))
      }).toVector))
    }
    def read(value: JsValue) =
      value.asJsObject.getFields("dag name","nodes") match {
        case Seq(JsString(dagName), JsArray(vector: Vector[JsValue])) =>
          vector.foldLeft(DAG(dagName))((dag, value2) => {
            value2.asJsObject.getFields("name", "id", "dependency", "condition") match {
              case Seq(JsString(name), JsNumber(nodeId), JsArray(dependency), JsString(condition)) =>
                val dependencyList = dependency.map {
                  case JsNumber(name) => name.toInt
                  case _ => throw new DeserializationException("Dependencies should contain names of node")
                }.toList
                dag.add(name, nodeId.toInt, dependencyList, condition, () => Map())
              case _ => throw new DeserializationException("Wrong description of node")
            }
          })
        case _ => throw new DeserializationException("No nodes in Json")
      }
  }
}



