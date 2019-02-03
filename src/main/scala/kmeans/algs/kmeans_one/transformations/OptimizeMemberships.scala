package kmeans.algs.kmeans_one.transformations

import kmeans.algs.common.Point
import kmeans.algs.common.TupleLike.PointWithMembership
import kmeans.algs.kmeans_one.TupleLike.Centroid
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

import scala.annotation.tailrec
import scala.collection.JavaConverters._


class OptimizeMemberships(varId: String) extends RichMapFunction[Point, PointWithMembership] {
  private var currentCentroids: Seq[Centroid] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    currentCentroids = getRuntimeContext.getBroadcastVariable[Centroid](varId).asScala
  }

  override def map(p: Point): PointWithMembership = {
    val nearestCentroid: Centroid = currentCentroids.head
    val minDist: Double = p distance nearestCentroid.point

    @tailrec
    def loop(currentNearest: Centroid, currDist: Double, centroids: Seq[Centroid]): Centroid = centroids match {
      case head +: tail =>
        val dist = p distance head.point
        if (dist < currDist) loop(head, dist, tail) else loop(currentNearest, currDist, tail)
      case _ =>
        currentNearest
    }

    PointWithMembership(loop(nearestCentroid, minDist, currentCentroids.tail).cluster, p)
  }
}
