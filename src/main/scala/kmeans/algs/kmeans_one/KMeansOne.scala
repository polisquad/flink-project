package kmeans.algs.kmeans_one

import kmeans.CmdLineParser.CmdArgs
import kmeans.Utils
import kmeans.algs.KMeansAlg
import kmeans.algs.common.Point
import kmeans.algs.common.TupleLike.PointWithMembership
import kmeans.algs.kmeans_one.TupleLike.Centroid
import kmeans.algs.kmeans_one.transformations.{CheckConvergence, OptimizeMemberships}
import org.apache.flink.api.scala.utils._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}


object KMeansOne extends KMeansAlg {
  val CurrentCentroids = "currentCentroids"
  val NewCentroids = "newCentroids"
  val FinalCentroids = "finalCentroids"
  val Cluster = "cluster"

  type PwmL = (PointWithMembership, Long)
  type CentroidLoss = (Int, Double)

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
      centroids.iterateWithTermination(cmdArgs.maxIterations)((currentCentroids: DataSet[Centroid]) => {

        // Compute new memberships
        val pwms: DataSet[PointWithMembership] =
          dataset
            .map(OptimizeMemberships(CurrentCentroids)).withBroadcastSet(currentCentroids, CurrentCentroids)

        // Compute new centroids
        val newCentroids: DataSet[Centroid] =
          pwms
            .map((pwm: PointWithMembership) => (pwm, 1L))
            .groupBy((t: PwmL) => t._1.cluster)
            .reduce((t1: PwmL, t2: PwmL) => (t1._1.copy(point = t1._1.point + t2._1.point), t1._2 + t2._2))
            .map((t: PwmL) => Centroid(t._1.cluster, t._1.point / t._2))

        // Compute loss per centroid
        val lossPerCentroid: DataSet[CentroidLoss] =
          pwms
            .join(newCentroids)
            .where(Cluster)
            .equalTo(Cluster)((pwm: PointWithMembership, c: Centroid) => (pwm.cluster, pwm.point squaredDistance c.point))
            .groupBy((t: CentroidLoss) => t._1)
            .reduce((t1: CentroidLoss, t2: CentroidLoss) => (t1._1, t1._2 + t2._2))

        // Compute overall new loss
        val newLoss: DataSet[Double] =
          lossPerCentroid
            .map((t: CentroidLoss) => t._2)
            .reduce((l1: Double, l2: Double) => l1 + l2)

        // Build termination
        val converged: DataSet[Boolean] =
          newLoss
            .reduceGroup(CheckConvergence(cmdArgs.tolerance))

        (newCentroids, converged)
      })

    // Build up final clustered dataset
    val clusteredDataset: DataSet[PointWithMembership] =
      dataset
        .map(OptimizeMemberships(FinalCentroids)).withBroadcastSet(finalCentroids, FinalCentroids)

    // Write results to file
    Utils.writeClusteredDatasetToFile(cmdArgs.output, clusteredDataset.collect())
  }
}
