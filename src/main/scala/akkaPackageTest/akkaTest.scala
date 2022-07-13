package akkaPackageTest

import akka.actor.*
import akka.cluster.{Cluster, ClusterEvent}
import akka.pattern.{ask, pipe}
import akka.remote.RemoteScope
import akka.util.Timeout
import akkaPackage.*
import akkaPackage.AkkaFlow.ResultFromNode
import akkaPackage.akkaFlowSystem.Check

import scala.concurrent.duration.DurationInt
import scala.io.Source.fromFile
import scala.language.postfixOps
import akkaPackage.DAG.generateDag


object akkaTest:
  case class Check(name : String)


class akkaTest extends Actor with ActorLogging:
  import context.dispatcher

  val cluster = Cluster(context.system)
  cluster.subscribe(self, classOf[ClusterEvent.MemberUp])
  val main = cluster.selfAddress.copy(port = Some(25520))
  cluster.join(main)


  val dags = Range(0, 10).map(_ => generateDag(10))

  var status = dags.map(dag => (dag.dagName, false)).toMap

  def start() : Receive =
    case ClusterEvent.MemberUp(member) =>
      if member.address == main then
        val system = context.actorOf(Props(classOf[akkaFlowSystem]).withDeploy(Deploy(scope = RemoteScope(member.address))))
        dags.foreach(dag => {
          system ! dag
          context.system.scheduler.scheduleOnce(5000.millis, system, Check(dag.dagName))
        })
        context.become(waitAnswers())


  def waitAnswers() : Receive =
    case Storage.Done(name) => {
      status = status + (name -> true)
      log.info(s"DAG $name successfully finished")
      if status.forall((_, bool) => bool) then
        log.info(s"Test successfully finished")
        context.stop(self)
    }
    case ClusterEvent.MemberUp(member) =>
    case msg => throw Error(msg.toString)

  context.setReceiveTimeout(10000.millis)
  override def receive: Receive = start()





