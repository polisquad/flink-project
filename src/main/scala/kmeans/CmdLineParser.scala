package kmeans

import org.apache.flink.api.java.utils.ParameterTool

import scala.util.{Failure, Success, Try}


object CmdLineParser {
  val NumClusters = "numClusters"
  val MaxIterations = "maxIterations"
  val Input = "input"
  val Output = "output"
  val Tolerance = "tolerance"

  val DefaultMaxIterations = 100
  val DefaultTolerance = 1e-2

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
      maxIterations <- Try(params.getInt(MaxIterations, DefaultMaxIterations))
      tolerance <- Try(params.getDouble(Tolerance, DefaultTolerance))
    } yield CmdArgs(input, output, k, maxIterations, tolerance)
  } match {
    case Success(cmdArgs) => cmdArgs
    case Failure(e) => throw new IllegalArgumentException(
      f"""
        |Something went wrong when parsing command line arguments.
        |>>> ${e.getMessage} <<<
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
