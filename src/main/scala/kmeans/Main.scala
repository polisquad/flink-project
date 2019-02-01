package kmeans

import kmeans.algs.KMeansOne
import org.apache.flink.api.java.utils.ParameterTool


object Main {

  def main(args: Array[String]): Unit = {
    implicit val params: ParameterTool = ParameterTool.fromArgs(args)
    KMeansOne.buildJob(CmdLineParser.getCmdArgs)
  }

}
