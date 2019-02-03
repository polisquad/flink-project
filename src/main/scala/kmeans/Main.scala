package kmeans

import kmeans.algs.kmeans_one.KMeansOne
import kmeans.algs.local_kmeans.LocalKMeans
import org.apache.flink.api.java.utils.ParameterTool


object Main {

  def main(args: Array[String]): Unit = {
    implicit val params: ParameterTool = ParameterTool.fromArgs(args)
    LocalKMeans.buildJob(CmdLineParser.getCmdArgs)
  }

}
