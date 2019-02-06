package kmeans.algs.local_kmeans

import kmeans.algs.common.Point


object TupleLike {
  case class LocalCentroid(cluster: Int, point: Point, numPoints: Long)
}
