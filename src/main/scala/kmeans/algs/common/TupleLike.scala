package kmeans.algs.common


object TupleLike {

  case class PointWithMembership(cluster: Int, point: Point) {
    def toCsv: String = point.toCsv + "," + cluster
  }

}
