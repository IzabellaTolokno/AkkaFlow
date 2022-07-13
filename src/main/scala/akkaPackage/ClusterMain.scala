package akkaPackage

import akka.actor.typed.ActorSystem
import com.typesafe.config.ConfigFactory

object ClusterMain extends App:
  def config(port : Int) = ConfigFactory.parseString(s"""
       akka.remote.artery.canonical.port = $port
        """).withFallback(ConfigFactory.load())
  val sys = ActorSystem(AkkaFlowSystem.behavior(), "Main", config(25520))

  ActorSystem(ClusterWorker.behavior(10), "Main", config(25521))
  ActorSystem(ClusterWorker.behavior(10), "Main", config(25522))

//  sys ! AkkaFlowSystem.DagRequest(DAG.generateDag(10))