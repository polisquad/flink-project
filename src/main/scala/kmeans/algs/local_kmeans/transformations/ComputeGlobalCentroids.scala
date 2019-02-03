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
class ComputeGlobalCentroids(parallelism: Int) extends RichGroupReduceFunction[LocalCentroid, Centroid] {

  @tailrec
  private def reduceIterator(curr: Point, numZeroed: Int, localCentroids: Iterator[LocalCentroid]): (Point, Int) = {
    if (localCentroids.hasNext) {
      val localCentroid = localCentroids.next
      reduceIterator(curr + localCentroid.point, if (localCentroid.isZero) numZeroed + 1 else numZeroed, localCentroids)
    }
    else (curr, numZeroed)
  }

  override def reduce(in: lang.Iterable[LocalCentroid], out: Collector[Centroid]): Unit = {
    val it = in.asScala.iterator

    if (it.hasNext) {
      val firstElement = it.next
      val (finalCentroidPoint, numZeroed) = reduceIterator(firstElement.point, 0, it)

      // Why parallelism - numZeroed? In case a partition did not contribute to a subset of the centroids
      out.collect(Centroid(firstElement.cluster, finalCentroidPoint / (parallelism - numZeroed)))
    }
  }
}

object ComputeGlobalCentroids {
  def apply(parallelism: Int): ComputeGlobalCentroids = new ComputeGlobalCentroids(parallelism)
}
