package kmeans.algs.local_kmeans.transformations

import java.lang

import kmeans.algs.common.Point
import kmeans.algs.kmeans_one.TupleLike.Centroid
import kmeans.algs.local_kmeans.TupleLike.LocalCentroid
import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.annotation.tailrec
import scala.collection.JavaConverters._



class Optimize(k: Int, varId: String) extends RichMapPartitionFunction[Point, LocalCentroid] {

  private var currentCentroids: Seq[Centroid] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    currentCentroids = getRuntimeContext.getBroadcastVariable(varId).asScala
  }

  def findNearestCluster(p: Point): Int = {
    val nearestCentroid: Centroid = currentCentroids.head
    val minDist: Double = p distance nearestCentroid.point

    @tailrec
    def loop(currentNearest: Centroid, currDist: Double, centroids: Seq[Centroid]): Centroid = centroids match {
      case head +: tail =>
        val dist: Double = p distance head.point
        if (dist < currDist) loop(head, dist, tail) else loop(currentNearest, currDist, tail)
      case _ =>
        currentNearest
    }

    loop(nearestCentroid, minDist, currentCentroids.tail).cluster
  }

  // Compute memberships of each point and compute local centroids according to local membership view
  override def mapPartition(points: lang.Iterable[Point], out: Collector[LocalCentroid]): Unit = {
    val it = points.asScala.iterator
    if (it.hasNext) {
      val newLocalCentroids: Array[Point] = Array.fill(k)(Point(Array.fill(currentCentroids.head.point.getDim)(0.0): _*))
      val numPointsPerCentroids: Array[Long] = Array.fill(k)(0)

      it.foreach((p: Point) => {
        val cluster: Int = findNearestCluster(p)
        newLocalCentroids(cluster) = newLocalCentroids(cluster) + p
        numPointsPerCentroids(cluster) += 1
      })

      for (i <- newLocalCentroids.indices) {
        out.collect(LocalCentroid(i, newLocalCentroids(i), numPointsPerCentroids(i)))
      }
    }
  }
}

object Optimize {
  def apply(k: Int, varId: String): Optimize = new Optimize(k, varId)
}
