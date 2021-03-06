package kmeans.algs.local_kmeans.transformations

import java.lang

import kmeans.algs.common.Point
import kmeans.algs.kmeans_one.TupleLike.Centroid
import kmeans.algs.local_kmeans.TupleLike.LocalCentroid
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.util.Collector

import scala.annotation.tailrec
import scala.collection.JavaConverters._


/**
  * Combine here is not needed since we have that the operators before this group reduce
  * produce one centroids per cluster
  */
class ComputeGlobalCentroids extends RichGroupReduceFunction[LocalCentroid, Centroid] {

  @tailrec
  private def reduceIterator(curr: Point, numPoints: Long, localCentroids: Iterator[LocalCentroid]): (Point, Long) = {
    if (localCentroids.hasNext) {
      val localCentroid = localCentroids.next()
      reduceIterator(curr + localCentroid.point, numPoints + localCentroid.numPoints, localCentroids)
    }
    else (curr, numPoints)
  }

  override def reduce(in: lang.Iterable[LocalCentroid], out: Collector[Centroid]): Unit = {
    val it = in.asScala.iterator

    if (it.hasNext) {
      val firstElement = it.next()
      val (finalCentroidPoint, numPoints) = reduceIterator(firstElement.point, firstElement.numPoints, it)
      out.collect(Centroid(firstElement.cluster, finalCentroidPoint / numPoints))
    }
  }
}

object ComputeGlobalCentroids {
  def apply(): ComputeGlobalCentroids = new ComputeGlobalCentroids()
}
