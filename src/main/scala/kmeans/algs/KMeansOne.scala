package kmeans.algs

import kmeans.CmdLineParser.CmdArgs
import kmeans.Utils
import kmeans.datatypes.Point
import kmeans.datatypes.TupleLike.{Centroid, Loss, PointWithMembership}
import kmeans.transformations.{CheckConvergence, OptimizeCentroids, OptimizeMemberships}
import org.apache.flink.api.scala.utils._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}


object KMeansOne extends KMeansAlg {
  val CurrentCentroids = "currentCentroids"
  val NewCentroids = "newCentroids"
  val FinalCentroids = "finalCentroids"
  val Cluster = "cluster"

  /**
    * High level overview of this Job:
    *   - Optimize membership
    *   - Group points by clusters
    *   - Optimize centroids
    *   - Compute overall loss
    */
  override def buildJob(cmdArgs: CmdArgs): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataset: DataSet[Point] = env.readTextFile(cmdArgs.input)
      .flatMap((datasetString: String) => datasetString.split("\n"))
      .map((pointString: String) => Point(pointString.split(",").map(_.toDouble): _*))

    val centroids: DataSet[Centroid] = dataset
      .first(cmdArgs.k)
      .zipWithIndex
      .map((t: (Long, Point)) => Centroid(t._1.toInt, t._2, 0))

    val finalCentroids: DataSet[Centroid] =
      centroids.iterateWithTermination(cmdArgs.maxIterations)((currentCentroids: DataSet[Centroid]) => {

        // Compure current loss
        val currentLoss: DataSet[Loss] =
          currentCentroids
            .map((c: Centroid) => Loss(CheckConvergence.OldLoss, c.loss))
            .reduce((l1: Loss, l2: Loss) => l1.copy(value = l1.value + l2.value))

        // Compute nearest centroids
        val pwms: DataSet[PointWithMembership] =
          dataset
            .map(new OptimizeMemberships(CurrentCentroids)).withBroadcastSet(currentCentroids, CurrentCentroids)

        // Compute new centroids
        val newCentroidsWithoutLoss: DataSet[Centroid] =
          pwms
            .groupBy((pwm: PointWithMembership) => pwm.cluster)
            .reduceGroup(new OptimizeCentroids())

        // Compute loss per centroid
        val newCentroids: DataSet[Centroid] =
          pwms.join(newCentroidsWithoutLoss)
            .where(Cluster)
            .equalTo(Cluster)((pwm: PointWithMembership, c: Centroid) =>
              Centroid(c.cluster, c.point, pwm.point squaredDistance c.point))
            .groupBy((c: Centroid) => c.cluster)
            .reduce((c1: Centroid, c2: Centroid) => c1.copy(c1.cluster, c1.point, c1.loss + c2.loss))

        // Compute overall new loss
        val newLoss: DataSet[Loss] =
          newCentroids
            .map((c: Centroid) => Loss(CheckConvergence.NewLoss, c.loss))
            .reduce((l1: Loss, l2: Loss) => l1.copy(value = l1.value + l2.value))

        // Build termination
        val converged: DataSet[Boolean] =
          currentLoss
            .union(newLoss)
            .reduceGroup(new CheckConvergence(cmdArgs.tolerance))
            .filter((b: Boolean) => b == false)

        (newCentroids, converged)
      })

    // Build up final clustered dataset
    val clusteredDataset: DataSet[PointWithMembership] =
      dataset
        .map(new OptimizeMemberships(FinalCentroids)).withBroadcastSet(finalCentroids, FinalCentroids)

    // Write results to file
    Utils.writeClusteredDatasetToFile(cmdArgs.output, clusteredDataset.collect())
  }
}
