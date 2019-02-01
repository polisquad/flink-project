package kmeans

import org.apache.flink.api.java.utils.ParameterTool

import scala.util.{Failure, Success, Try}


object CmdLineParser {
  val NumClusters = "numClusters"
  val MaxIterations = "maxIterations"
  val Input = "input"
  val Output = "output"
  val Tolerance = "tolerance"

  case class CmdArgs(input: String,
                     output: String,
                     k: Int,
                     maxIterations: Int,
                     tolerance: Double)

  def getCmdArgs(implicit params: ParameterTool): CmdArgs = {
    for {
      input <- Try(params.get(Input))
      output <- Try(params.get(Output))
      k <- Try(params.getInt(NumClusters))
      maxIterations <- Try(params.getInt(MaxIterations))
      tol <- Try(params.getDouble(Tolerance))
    } yield CmdArgs(input, output, k, maxIterations, tol)
  } match {
    case Success(cmdArgs) => cmdArgs
    case Failure(_) => throw new IllegalArgumentException(
      """
        |Something went wrong when parsing command line arguments.
        |Usage:
        | --input where to read the input dataset
        | --output where to write the clustered dataset
        | --k the number of clusters
        | --maxIterations the max number of iterations
        | --tol the tolerance used when checking for convergence
      """.stripMargin
    )
  }


}
