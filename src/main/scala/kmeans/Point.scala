package kmeans

import scala.annotation.tailrec

/**
  * N-dimensional data point
  */
class Point private (private val data: Array[Double]) {

  /**
    * General operation to apply f between two Point
    */
  private def op(other: Point)(f: (Double, Double) => Double): Point = {
    val outData: Array[Double] = Array.ofDim(this.data.length)
    for (i <- data.indices)
      outData(i) = f(data(i), other.data(i))
    Point(outData)
  }

  /**
    * General operation to apply f between a Point and a scalar
    */
  private def op(scalar: Double)(f: (Double, Double) => Double): Point = {
    val outData: Array[Double] = Array.ofDim(this.data.length)
    for (i <- data.indices)
      outData(i) = f(data(i), scalar)
    Point(outData)
  }

  /**
    * General operation to apply f to each component
    */
  private def op(f: Double => Double): Point = {
    val outData: Array[Double] = Array.ofDim(this.data.length)
    for (i <- data.indices)
      outData(i) = f(data(i))
    Point(outData)
  }

  // Point scalar ops
  def +(scalar: Double): Point = op(scalar)(_ + _)
  def -(scalar: Double): Point = op(scalar)(_ - _)
  def *(scalar: Double): Point = op(scalar)(_ * _)
  def /(scalar: Double): Point = op(scalar)(_ / _)

  // Point Point ops
  def +(other: Point): Point = op(other)(_ + _)
  def -(other: Point): Point = op(other)(_ - _)
  def *(other: Point): Point = op(other)(_ * _)
  def /(other: Point): Point = op(other)(_ / _)

  // Elementwise ops
  def ^(powerOf: Double): Point = op(c => math.pow(c, powerOf))

  def squaredDistance(other: Point): Double = math.sqrt(((this - other) ^ 2).data.sum)

  def ==(other: Point): Boolean = {
    @tailrec
    def loop(i: Int): Boolean = {
      if (i >= this.data.length) true
      else if (math.abs(data(i) - other.data(i)) >= Point.Tolerance) false
      else loop(i+1)
    }
    loop(0)
  }

  def !=(other: Point): Boolean = !(this == other)

  override def toString: String = {
    "Point(" + data.map(_.toString).mkString(", ") + ")"
  }
}

object Point {
  val Tolerance: Double = 1e-14

  def apply(data: Double*): Point = new Point(Array(data:_*))
  private def apply(data: Array[Double]): Point = new Point(data)

  implicit class ScalarOps (val scalar: Double) extends AnyVal {
    def +(point: Point): Point = point + scalar
    def *(point: Point): Point = point * scalar
    def -(point: Point): Point = point - scalar
    def /(point: Point): Point = point / scalar
  }

}

