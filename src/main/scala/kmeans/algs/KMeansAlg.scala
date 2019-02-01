package kmeans.algs

import kmeans.CmdLineParser.CmdArgs


trait KMeansAlg {

  /**
    * Builds a Flink Job of the k-means algorithm.
    */
  def buildJob(cmd: CmdArgs): Unit

}
