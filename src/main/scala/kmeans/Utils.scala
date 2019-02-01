package kmeans

import java.io.PrintWriter

import kmeans.datatypes.Point
import kmeans.datatypes.TupleLike.PointWithMembership

import scala.util.Random


object Utils {

  def createRandomDataset(n: Int, size: Int): Vector[Point] = {
    Vector.fill(size)(Point(Array.fill(n)(new Random().nextDouble() * 10): _*))
  }

  def writeDatasetToFile(file: String, dataset: Vector[Point]): Unit = {
    val pw: PrintWriter = new PrintWriter(file)

    try {
      dataset.foreach(p => pw.println(p.toCsv))
    } finally {
      pw.close()
    }
  }

  def writeClusteredDatasetToFile(file: String, dataset: Seq[PointWithMembership]): Unit = {
    val pw: PrintWriter = new PrintWriter(file)

    try {
      dataset.foreach(pwm => pw.println(pwm.toCsv))
    } finally {
      pw.close()
    }
  }

}
