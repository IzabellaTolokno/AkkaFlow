import akka.actor.{ActorRef, ActorSystem, Props}
import akkaPackage.AkkaFlowNode
import akkaPackage.AkkaFlow
import akkaPackage.DAG
import akka.testkit.{ImplicitSender, TestKit, TestKitBase}

import concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.Random
import java.time.LocalDateTime
import scala.io.Source.fromFile
import spray.json._
import akkaPackage.MyJsonProtocol.ColorJsonFormat

class akkaFlowTest extends munit.FunSuite{
  Impl()
  class Impl
    extends TestKit(ActorSystem("BinaryTreeSuite"))
      with ImplicitSender:
    test("akkaFlow") {
      var list = List[ActorRef]()
      val dags = Range(0, 9).map(j => fromFile(s"C:/Users/izabella.tolokno/example/AkkaFlow/src/test/scala/DAGs/file$j.txt")
        .mkString.parseJson.convertTo[DAG])
      for dag <- dags do list = List(system.actorOf(Props(classOf[AkkaFlow], dag, self))) ++ list
      for _ <- dags do expectMsg(10000 millis, "Done")
    }


}
