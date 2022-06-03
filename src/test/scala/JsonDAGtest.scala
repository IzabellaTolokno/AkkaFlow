import akkaPackage.DAG

import scala.io.Source.fromFile
import java.io.{File, PrintWriter}
import spray.json.*
import akkaPackage.MyJsonProtocol.ColorJsonFormat

import scala.util.Random

class JsonDAGtest extends munit.FunSuite {
  test("JsonDAG") {
    val lines = fromFile("C:/Users/izabella.tolokno/example/AkkaFlow/src/test/scala/file.txt").mkString.parseJson
    val dag = lines.convertTo[DAG].toJson
    assert(lines == dag)
  }

  def generateDags(quantityDags : Int, quantityNodes : Int): Unit =
    for j <- Range(0, quantityDags) do
      val writer = new PrintWriter(new File(s"C:/Users/izabella.tolokno/example/AkkaFlow/src/test/scala/DAGs/file$j.txt"))
      val dag = DAG()
      for i <- Range(0, quantityNodes) do {
        val nDependency = if i > 2 then math.min(Random.nextInt(i - 1) + 1, 50) else 0
        var dependency = Set[Int]()
        for _ <- Range(0, nDependency) do {
          dependency = dependency + Random.nextInt(i)
        }
        dag.add("Node " + i.toString, i, dependency.toList,
          "all_success",
          () => Map[Int, Map[String, String]]())
      }
      writer.write(dag.toJson.prettyPrint)
      writer.close()

  test("Generate DAGs"){
    generateDags(10, 100)
  }
}
