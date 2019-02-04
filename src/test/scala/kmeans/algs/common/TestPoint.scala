package kmeans.algs.common

import org.scalactic.Equality
import org.scalatest.FunSuite


class TestPoint extends FunSuite {

  implicit val pointEqual: Equality[Point] = (a: Point, b: Any) => b match {
    case p: Point => a == p
    case _ => false
  }

  implicit val doubleEqual: Equality[Double] = (a: Double, b: Any) => b match {
    case d: Double => math.abs(a - d) < Point.Tolerance
    case _ => false
  }

  test("point ops") {

    val p1 = Point(1, 2, 3)
    val p2 = Point(4, 5, 6)

    assert(p1.getDim === 3)
    assert(p1 * 3 === Point(3, 6, 9))
    assert(p2 === p2)
    assert((p1 ^ 3) === Point(1, 8, 27))
    assert((p1 distance p2) === 5.19615242270663)
    assert((Point(1, 3) distance Point(4, 7)) === 5.0)
  }
}
