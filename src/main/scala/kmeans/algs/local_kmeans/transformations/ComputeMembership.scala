package kmeans.algs.local_kmeans.transformations

import java.lang

import kmeans.algs.common.Point
import kmeans.algs.common.TupleLike.PointWithMembership
import kmeans.algs.kmeans_one.TupleLike.Centroid
import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.annotation.tailrec
import scala.collection.JavaConverters._


class ComputeMembership(varId: String) extends RichMapPartitionFunction[Point, PointWithMembership] {
  private var centroids: Seq[Centroid] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    centroids = getRuntimeContext.getBroadcastVariable(varId).asScala
  }

  def findNearest(p: Point): PointWithMembership = {
    val nearestCentroid: Centroid = centroids.head
    val minDist: Double = p distance nearestCentroid.point

    @tailrec
    def loop(currentNearest: Centroid, currDist: Double, centroids: Seq[Centroid]): Centroid = centroids match {
      case head +: tail =>
        val dist = p distance head.point
        if (dist < currDist) loop(head, dist, tail) else loop(currentNearest, currDist, tail)
      case _ =>
        currentNearest
    }

    PointWithMembership(loop(nearestCentroid, minDist, centroids.tail).cluster, p)
  }

  override def mapPartition(values: lang.Iterable[Point], out: Collector[PointWithMembership]): Unit = {
    values.forEach((p: Point) => out.collect(findNearest(p)))
  }
}

object ComputeMembership {
  def apply(varId: String): ComputeMembership = new ComputeMembership(varId)
}
