import akka.actor.{ActorRef, ActorSystem, OneForOneStrategy, Props, SupervisorStrategy}
import akkaPackage.*
import akka.testkit.{ImplicitSender, TestKit, TestKitBase}

import concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.Random
import java.time.LocalDateTime
import scala.io.Source.fromFile
import spray.json.*
import akkaPackage.MyJsonProtocol.ColorJsonFormat
import akkaPackage.akkaFlowSystem.Check

class akkaFlowTest extends munit.FunSuite{
  Impl()
  class Impl
    extends TestKit(ActorSystem("BinaryTreeSuite"))
      with ImplicitSender:
    test("akkaFlow") {
      val countDag = 1
      val dags = Range(0, countDag).map(j => fromFile(s"C:/Users/izabella.tolokno/example/AkkaFlow/src/test/scala/DAGs/file$j.txt")
        .mkString.parseJson.convertTo[DAG])
      val sys = system.actorOf(Props(classOf[akkaFlowSystem], self))
      for dag <- dags do sys ! dag
      Thread.sleep(5000)
      for dag <- dags do
        sys ! Check(dag.dagName)
        expectMsg(10000 millis, Done(dag.dagName))
    }
}
