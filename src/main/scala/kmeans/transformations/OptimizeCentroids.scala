package kmeans.transformations

import java.lang

import kmeans.datatypes.Point
import kmeans.datatypes.TupleLike.{Centroid, PointWithMembership}
import org.apache.flink.api.common.functions.{CombineFunction, GroupReduceFunction}
import org.apache.flink.util.Collector

import scala.annotation.tailrec
import scala.collection.JavaConverters._


class OptimizeCentroids extends GroupReduceFunction[PointWithMembership, Centroid]
  with CombineFunction[PointWithMembership, PointWithMembership] {

  @tailrec
  private def reduceIterator(curr: Point, count: Int, pwms: Iterator[PointWithMembership]): Point = {
    if (pwms.hasNext) reduceIterator(curr + pwms.next().point, count + 1, pwms)
    else curr / count
  }

  override def combine(in: lang.Iterable[PointWithMembership]): PointWithMembership = {
    val pwms: Iterator[PointWithMembership] = in.iterator.asScala
    val firstElement: PointWithMembership = pwms.next
    PointWithMembership(firstElement.cluster, reduceIterator(firstElement.point, 1, pwms))
  }

  override def reduce(in: java.lang.Iterable[PointWithMembership], out: Collector[Centroid]): Unit = {
    val pwms: Iterator[PointWithMembership] = in.iterator.asScala
    val firstElement: PointWithMembership = pwms.next()
    out.collect(Centroid(firstElement.cluster, reduceIterator(firstElement.point, 1, pwms), 0))
  }
}