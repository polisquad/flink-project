package kmeans.datatypes


object TupleLike {

  case class PointWithMembership(cluster: Int, point: Point) {
    def toCsv: String = point.toCsv + "," + cluster
  }

  case class Centroid(cluster: Int, point: Point, loss: Double)

  case class Loss(kind: String, value: Double)

}
