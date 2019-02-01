package kmeans.transformations

import java.lang

import kmeans.datatypes.TupleLike.Loss
import org.apache.flink.api.common.accumulators.{IntCounter, ListAccumulator}
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._


class CheckConvergence(tol: Double) extends RichGroupReduceFunction[Loss, Boolean] {
  import CheckConvergence._

  private val numEpoch: IntCounter = new IntCounter(0)
  private val losses: ListAccumulator[Double] = new ListAccumulator[Double]()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    getIterationRuntimeContext.addAccumulator(NumEpoch, numEpoch)
    getIterationRuntimeContext.addAccumulator(Losses, losses)
  }

  override def reduce(in: lang.Iterable[Loss], out: Collector[Boolean]): Unit = {
    val it = in.asScala.iterator
    val l1 = it.next()
    val l2 = it.next()
    numEpoch.add(1)
    losses.add(if (l1.kind == NewLoss) l1.value else l2.value)
    out.collect(math.abs(l1.value - l2.value) <= tol)
  }

}

object CheckConvergence {
  val NumEpoch: String = "numEpochs"
  val Losses: String = "losses"
  val OldLoss: String = "oldLoss"
  val NewLoss: String = "newLoss"
}