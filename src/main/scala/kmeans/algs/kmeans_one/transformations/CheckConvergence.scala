package kmeans.algs.kmeans_one.transformations

import java.lang

import org.apache.flink.api.common.accumulators.{IntCounter, ListAccumulator}
import org.apache.flink.api.common.functions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class CheckConvergence(tolerance: Double) extends RichGroupReduceFunction[Double, Boolean] {
  import CheckConvergence._

  private val numEpoch = new IntCounter(0)
  private val losses = new ListAccumulator[Double]()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    getIterationRuntimeContext.addAccumulator(NumEpoch, numEpoch)
    getIterationRuntimeContext.addAccumulator(Losses, losses)
  }

  override def reduce(newLoss: lang.Iterable[Double], out: Collector[Boolean]): Unit = {
    val historyLoss = losses.getLocalValue
    val historyLength = historyLoss.size()
    val old = if (historyLength > 0) historyLoss.get(historyLength - 1) else 0
    val neww = newLoss.iterator().next()
    losses.add(neww)
    numEpoch.add(1)
    if (math.abs(old - neww) > tolerance) out.collect(false)
  }
}

object CheckConvergence {
  val NumEpoch = "numEpoch"
  val Losses = "losses"

  def apply(tolerance: Double): CheckConvergence = new CheckConvergence(tolerance)
}
