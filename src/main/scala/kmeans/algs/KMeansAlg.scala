package kmeans.algs

import kmeans.CmdLineParser.CmdARgs


trait KMeansAlg {

  /**
    * Builds a Flink Job of the k-means algorithm.
    */
  def buildJob(cmd: CmdARgs): Unit

}
