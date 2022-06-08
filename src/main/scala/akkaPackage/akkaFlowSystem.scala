package akkaPackage

import akka.actor.*
import akkaPackage.*
import akkaPackage.AkkaFlow.ResultFromNode
import akkaPackage.akkaFlowSystem.Check
import concurrent.duration.DurationInt
import concurrent.ExecutionContext.Implicits.global
import akka.actor.typed.Dispatchers

import scala.language.postfixOps

object akkaFlowSystem:
  case class Check(name : String)


class akkaFlowSystem(parent : ActorRef) extends Actor:
  override val supervisorStrategy = OneForOneStrategy(){
    case _ => SupervisorStrategy.Restart
  }
  val storage = context.actorOf(Props(classOf[Storage], List[String](), Map[String, Map[Int, ResultFromNode]](), Map[String, Status]()))

  var scheduler : List[Cancellable] = List()

  override def receive =
    case dag : DAG => context.actorOf(Props(classOf[AkkaFlow], dag,self, storage), dag.dagName)
    case Check(name : String) => context.system.scheduler.scheduleAtFixedRate(0 millis, 1000 millis,
      storage, Ask(name))
    case info : Status => parent ! info


