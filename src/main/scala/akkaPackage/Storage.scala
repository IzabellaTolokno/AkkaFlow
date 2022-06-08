package akkaPackage

import akka.actor.{Actor, ActorRef}
import akka.routing.ActorRefRoutee
import akkaPackage.AkkaFlow.ResultFromNode

case class Subscribe(name : String)

case class NewResult(name : String, resultFromNode: ResultFromNode)

case class Restart(name : String)

case class Results(list: Map[Int, ResultFromNode])

sealed trait Status:
  val name : String
case class Done(name : String) extends Status
case class NotDone(name : String) extends Status

case class Ask(name : String)


class Storage(var subscribers : List[String],
              var description : Map[String, Map[Int, ResultFromNode]] = Map[String, Map[Int, ResultFromNode]](),
              var status :  Map[String, Status] = Map[String, Status]()) extends Actor:

  def receive =
    case Done(name : String) => status = status + (name -> Done(name))
    case NewResult(name, result : ResultFromNode) => description = description + (name -> (description.getOrElse(name, Map[Int, ResultFromNode]()) + (result.fromId -> result)))
    case Restart(name) => sender() ! Results(description.getOrElse(name, Map[Int, ResultFromNode]()))
    case Ask(name : String) => sender() ! status.getOrElse(name, NotDone(name))