package akkaPackage

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.Actor
import akka.routing.ActorRefRoutee
import akkaPackage.AkkaFlow.{Command, ResultFromNode}
import akkaPackage.Storage.*
import akkaPackage.AkkaFlowSystem.{Command, SystemStatus}

object Storage:

  sealed trait Command

  case class NewResult(name : String, resultFromNode: ResultFromNode) extends Command

  case class Restart(name : String) extends Command

  sealed trait Status extends Command:
    val name : String
  case class Done(name : String) extends Status
  case class NotDone(name : String) extends Status
  case class Unknown(name : String) extends Status
  case class AskStatus(name : String) extends Command
  case class StorageMessageWithSender[T](message: Command, sender : ActorRef[T]) extends Command


  def behavior() : Behavior[Command] = Behaviors.setup { context =>
    var description : Map[String, Map[Int, ResultFromNode]] = Map[String, Map[Int, ResultFromNode]]()
    var status :  Map[String, Storage.Status] = Map[String, Storage.Status]()
    Behaviors.receiveMessagePartial { msg =>
      msg match
        case Done(name: String) => status = status + (name -> Done(name))
        case NewResult(name, result: ResultFromNode) => description = description + (name -> (description.getOrElse(name, Map[Int, ResultFromNode]()) + (result.fromId -> result)))
        case StorageMessageWithSender(Restart(name), sender: ActorRef[AkkaFlow.Command]) => sender ! AkkaFlow.Results(description.getOrElse(name, Map[Int, ResultFromNode]()))
        case StorageMessageWithSender(AskStatus(name: String), sender: ActorRef[AkkaFlowSystem.Command]) =>
          sender ! SystemStatus(status.getOrElse(name, NotDone(name)))
        case msg => println(s"Unknown message $msg")
      Behaviors.same
    }
  }

