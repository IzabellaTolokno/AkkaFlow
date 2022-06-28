import akkaPackage.DAG
import akkaPackage.DAG.generateDag
import akkaPackage.MyJsonProtocol.ColorJsonFormat
import spray.json.*

import java.io.{File, PrintWriter}
import scala.io.Source.fromFile
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

  test("Generate DAGs"){
    val quantityDags = 10
    Range(0, quantityDags).foreach(j => {
      val writer = new PrintWriter(new File(s"C:/Users/izabella.tolokno/example/AkkaFlow/src/test/scala/DAGs/file$j.txt"))
      writer.write(generateDag(10).toJson.prettyPrint)
      writer.close()
    })
  }
}