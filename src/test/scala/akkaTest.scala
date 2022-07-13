import akka.actor.ReceiveTimeout
import akka.actor.typed.ActorSystem
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent
import akka.cluster.typed.{Cluster, Join}
import akkaPackage.AkkaFlowSystem.Check
import akkaPackage.DAG.generateDag
import akkaPackage.{AkkaFlowSystem, Storage}
import akka.actor.ReceiveTimeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.*
import java.time.Duration
import scala.language.postfixOps

object akkaTest extends App:
  val dags = Range(0, 10).map(_ => generateDag(100))

  case class ListingResponse(listing: Receptionist.Listing)

  def behavior() = Behaviors.setup{ context =>
    var status = dags.map(dag => (dag.dagName, false)).toMap

    val cluster = Cluster(context.system)
    val main = cluster.selfMember.address.copy(port = Some(25520))
    cluster.manager ! Join(main)

    context.system.receptionist ! Receptionist.Find(AkkaFlowSystem.SystemServiceKey, context.self)

    def waitListing() = Behaviors.receiveMessagePartial { msg =>
      msg match
        case AkkaFlowSystem.SystemServiceKey.Listing(listings) if listings.nonEmpty =>
          listings.foreach(ps => dags.foreach(dag => {
            ps ! AkkaFlowSystem.DagRequest(dag)
            context.system.scheduler.scheduleOnce(Duration.ofSeconds(5), new Runnable (){
              def run(): Unit = ps ! Check(dag.dagName, context.self)
            }, context.executionContext)
          }))
          context.log.info(s"Dags are sent")
          waitAnswers()
        case AkkaFlowSystem.SystemServiceKey.Listing(listings) if listings.isEmpty =>
          context.system.scheduler.scheduleOnce(Duration.ofSeconds(1), new Runnable (){
            def run(): Unit = context.system.receptionist ! Receptionist.Find(AkkaFlowSystem.SystemServiceKey, context.self)
          }, context.executionContext)
          Behaviors.same
        case ReceiveTimeout => throw Error("Timeout")
        case _ =>
          context.log.info(msg.toString)
          Behaviors.same
    }

    def waitAnswers() = Behaviors.receiveMessagePartial { msg =>
      msg match
        case Storage.Done(name) =>
          status = status + (name -> true)
          context.log.info(s"DAG $name successfully finished")
          if status.forall((_, bool) => bool) then
            context.log.info(s"Test successfully finished")
            Behaviors.stopped
          else
            Behaviors.same
        case ClusterEvent.MemberUp(member) => Behaviors.same
        case msg => throw Error(msg)
    }

    context.setReceiveTimeout(10000 milliseconds, "No answer")
    waitListing()
  }

  def config(port : Int) = ConfigFactory.parseString(s"""
       akka.remote.artery.canonical.port = $port
        """).withFallback(ConfigFactory.load())

  val sys = ActorSystem(behavior(), "Main", config(10))
