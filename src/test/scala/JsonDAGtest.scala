import akkaPackage.DAG

import scala.io.Source.fromFile
import java.io.{File, PrintWriter}
import spray.json.*
import akkaPackage.MyJsonProtocol.ColorJsonFormat

import scala.util.Random

class JsonDAGtest extends munit.FunSuite {
  test("JsonDAG") {
    val lines = fromFile("C:/Users/izabella.tolokno/example/AkkaFlow/src/test/scala/file.txt").mkString.parseJson
    val dag = lines.convertTo[DAG]
    val linesAgain = dag.toJson
    val dagAgain = linesAgain.convertTo[DAG]
    assert(lines == linesAgain)
    assert(dag.dagDown == dagAgain.dagDown)
  }

  def generateDags(quantityDags : Int, quantityNodes : Int): Unit =
    for j <- Range(0, quantityDags) do
      val writer = new PrintWriter(new File(s"C:/Users/izabella.tolokno/example/AkkaFlow/src/test/scala/DAGs/file$j.txt"))
      val dag = Range(0, quantityNodes).foldLeft(DAG(Iterator.continually(Random.nextPrintableChar)
        .filter(_.isLetter)
        .take(15)
        .mkString)) ((dag, i) => {
        val nDependency = if i > 2 then math.min(Random.nextInt(i - 1) + 1, 50) else 0
        val dependency = Range(0, nDependency).map(_ => Random.nextInt(i)).toSet
        dag.add("Node_" + i.toString + "_dag_" + dag.dagName, i, dependency.toList,
          "all_success",
          () => Map[Int, Map[String, String]]())
      })
      writer.write(dag.toJson.prettyPrint)
      writer.close()

  test("Generate DAGs"){
    generateDags(10, 10)
  }
}
