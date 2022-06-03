package akkaPackage

import akkaPackage.AkkaFlow.{DoneToNode, ErrorToNode, NotDoneToNode, ResultToNode}
import com.sun.xml.internal.ws.encoding.soap.DeserializationException

import scala.io.Source.fromFile
import spray.json.{JsArray, *}


class DAG:
  var Ids = Set[Int]()
  var dagUp = Map[Int, List[Int]]().withDefaultValue(List[Int]())
  var dagDown = Map[Int, List[Int]]().withDefaultValue(List[Int]())
  var nameToInt = Map[String, Int]()
  var intToName = Map[Int, String]()
  var intToDoing = Map[Int, () => Map[Int, Map[String, String]]]()
  var conditions = Map[Int, Map[Int, ResultToNode] => Boolean]()
  var conditionSkipped = Map[Int, Map[Int, ResultToNode] => Boolean]()
  var conditionsName = Map[Int, String]()

  def add(name: String, id: Int, dependency: List[Int], condition: String,
          doing : () => Map[Int, Map[String, String]]) =
    Ids = Ids + id
    dagUp = dagUp + (id -> dependency)
    for node <- dependency do
      dagDown = dagDown + (node -> (List(id) ++ dagDown.getOrElse(node,  List[Int]())))
    nameToInt = nameToInt + (name -> id)
    intToName = intToName + (id -> name)

    conditionsName = conditionsName + (id -> condition)
    if condition == "all_success" then
      conditions = conditions + (id -> (m => dependency.forall(x => m(x) match
        case DoneToNode(_, _, _) => true
        case _ => false)))
      conditionSkipped = conditionSkipped + (id -> (m => dependency.exists(x => m(x) match
        case DoneToNode(_, _, _) => false
        case NotDoneToNode(_, _) => false
        case _ => true)))

    if condition == "one_success" then
      conditions = conditions + (id -> (m => dependency.exists(x => m(x) match
        case DoneToNode(_, _, _) => true
        case _ => false)))
      conditionSkipped = conditionSkipped + (id -> (m => dependency.forall(x => m(x) match
        case DoneToNode(_, _, _) => false
        case NotDoneToNode(_, _) => false
        case _ => true)))

    if condition == "all_failed" then
      conditions = conditions + (id -> (m => dependency.forall(x => m(x) match
        case ErrorToNode(_, _, _) => true
        case NotDoneToNode(_, _) => false
        case _ => false)))
      conditionSkipped = conditionSkipped + (id -> (m => dependency.exists(x => m(x) match
        case ErrorToNode(_, _, _) => false
        case NotDoneToNode(_, _) => false
        case _ => true)))

    if condition == "one_failed" then
      conditions = conditions + (id -> (m => dependency.exists(x => m(x) match
        case ErrorToNode(_, _, _) => true
        case NotDoneToNode(_, _) => false
        case _ => false)))
      conditionSkipped = conditionSkipped + (id -> (m => dependency.forall(x => m(x) match
        case ErrorToNode(_, _, _) => false
        case NotDoneToNode(_, _) => false
        case _ => true)))

    intToDoing = intToDoing + (id -> doing)

  def isEmpty(id : Int) = dagDown(id).isEmpty

  def down(id : Int) =
    dagDown(id)

  def up(id : Int) = dagUp(id)

  def condition(id : Int) = conditions(id)

  def nodeSet() = Ids

  def doing(id : Int) = intToDoing(id)


class DagNode(val dagUp : List[Int], val dagDown : List[Int], val id : Int, val condition: Map[Int, ResultToNode] => Boolean,
              val conditionSkipped: Map[Int, ResultToNode] => Boolean, val doing : () => Map[Int, Map[String, String]])



object MyJsonProtocol extends DefaultJsonProtocol {
  implicit object ColorJsonFormat extends RootJsonFormat[DAG] {
    def write(dag : DAG) = {
      JsObject("nodes" -> JsArray(dag.Ids.map(id => {
        JsObject(
          "name" -> JsString(dag.intToName(id)),
          "id" -> JsNumber(id),
          "dependency" -> JsArray(dag.dagUp(id).map(x => JsNumber(x)).toVector),
          "condition" -> JsString(dag.conditionsName(id)))
      }).toVector))
    }
    def read(value: JsValue) =
      val dag = new DAG()
      value.asJsObject.getFields("nodes") match {
        case Seq(JsArray(vector: Vector[JsValue])) =>
          for value2 <- vector do {
            value2.asJsObject.getFields("name", "id", "dependency", "condition") match {
              case Seq(JsString(name), JsNumber(id), JsArray(dependency), JsString(condition)) =>
                val dependencyList = dependency.map {
                  case JsNumber(name) => name.toInt
                  case _ => throw new DeserializationException("Dependencies should contain names of node")
                }.toList
                dag.add(name, id.toInt, dependencyList, condition, () => Map())

              case _ => throw new DeserializationException("Wrong description of node")
            }
          }
          dag
        case _ => throw new DeserializationException("No nodes in Json")
      }
  }
}



