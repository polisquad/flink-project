package kmeans.algs.local_kmeans

import kmeans.CmdLineParser.CmdArgs
import kmeans.Utils
import kmeans.algs.KMeansAlg
import kmeans.algs.common.Point
import kmeans.algs.common.TupleLike.PointWithMembership
import kmeans.algs.kmeans_one.TupleLike.Centroid
import kmeans.algs.local_kmeans.TupleLike.LocalCentroid
import kmeans.algs.local_kmeans.transformations.{ComputeGlobalCentroids, ComputeMembership, Optimize}
import org.apache.flink.api.scala.utils._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}


object LocalKMeans extends KMeansAlg {
  val CurrentCentroids = "currentCentroids"
  val FinalCentroids = "finalCentroids"

  override def buildJob(cmdArgs: CmdArgs): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataset: DataSet[Point] = env.readTextFile(cmdArgs.input)
      .flatMap((datasetString: String) => datasetString.split("\n"))
      .map((pointString: String) => Point(pointString.split(",").map(_.toDouble): _*))

    val centroids: DataSet[Centroid] = dataset
      .first(cmdArgs.k)
      .zipWithIndex
      .map((t: (Long, Point)) => Centroid(t._1.toInt, t._2))

    val finalCentroids: DataSet[Centroid] =
      centroids.iterate(cmdArgs.maxIterations)((currentCentroids: DataSet[Centroid]) => {
        // Compute new centroids
        dataset
          .mapPartition(new Optimize(cmdArgs.k, CurrentCentroids)).withBroadcastSet(currentCentroids, CurrentCentroids)
          .groupBy((lc: LocalCentroid) => lc.cluster)
          .reduceGroup(new ComputeGlobalCentroids(env.getParallelism))
      })

    // Build up final clustered dataset
    val clusteredDataset: DataSet[PointWithMembership] =
      dataset
        .mapPartition(new ComputeMembership(FinalCentroids)).withBroadcastSet(finalCentroids, FinalCentroids)

    // Write results to file
    Utils.writeClusteredDatasetToFile(cmdArgs.output, clusteredDataset.collect())
  }
}
