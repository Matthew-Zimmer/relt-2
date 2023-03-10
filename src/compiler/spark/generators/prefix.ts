import { Project } from "../../../project";

export function makeSparkHeader(project: Project) {
  return `\
package ${project.name}

import org.apache.spark.sql.{ Encoders, Dataset, Row, SparkSession, Column, DataFrame, SaveMode }
import org.apache.spark.sql.functions.{ collect_list, struct, sum, lit, udf, col }
import java.sql.Date
`;
}

export function makeSparkPrefix(project: Project) {
  return `\
trait Instruction {
  def execute(spark: SparkSession, dss: DeltaTypes.Datasets): DeltaTypes.Datasets
}

trait Node {}
case class RefreshNode(table: String) extends Node {}
case class HashRefreshNode(table: String) extends Node {}
case class AllNode(nodes: Array[Node]) extends Node {}

class Vertex(
  val name: String,
  val incoming: Seq[String],
  val outgoing: Seq[String],
  val hasStorage: Boolean,
) {
  def isSource(): Boolean = hasStorage && incoming.size == 0 
  def hasCache(): Boolean = hasStorage && incoming.size != 0
}

class Dag(
  private val vertices: Map[String, Vertex],
  private val instructionSet: Map[String, Map[String, Instruction]],
) {
  def plan(seenTables: Seq[String]): Seq[Instruction] = {
    val vertices = this.topologicalOrdering(seenTables)
    return vertices.flatMap(this.instructionFor)
  }

  def instructionFor(v: Vertex): Seq[Instruction] = {
    if (v.isSource) {
      return Seq(
        this.instructionSet(v.name)("fetch"),
      )
    }
    else if (v.hasStorage) {
      return Seq(
        this.instructionSet(v.name)("derive"),
        this.instructionSet(v.name)("store"),
      )
    }
    else {
      return Seq(
        this.instructionSet(v.name)("derive"),
      )
    }
  }

  private def topologicalOrdering(without: Seq[String]): Seq[Vertex] = {
    val visited = scala.collection.mutable.Map[String, Boolean](this.vertices.keys.map(k => (k, false)).toSeq :_*)
    var stack = Seq[String]()
    
    def imp(id: String): Unit = {
      visited(id) = true
      
      for (i <- this.resolve(id).outgoing if (!visited.get(i).get))
        imp(i)
        
      stack +:= id
    }
    
    for (id <- this.vertices.keys)
      if (!visited.get(id).get)
        imp(id)
      
    return this.resolveMany(stack.filter(x => !(without contains x)))
  }

  private def resolve(name: String): Vertex = this.vertices(name)
  private def resolveMany(names: Seq[String]): Seq[Vertex] = names.map(this.resolve)
}

class Interpreter(
  private val dag: Dag, 
  private val instructionSet: Map[String, Map[String, Instruction]]
) {
  def parse(arg: String): Node = {
    val clean = arg
      .replaceAll(" +", " ")
      .replaceAll("\\n", " ")
      .replaceAll("\\t", " ")
    
    val parts = clean.split(" ")

    if (parts.size == 2 && parts(0) == "refresh")
      return new RefreshNode(parts(1))
    else
      throw new Exception(s"Bad: \${arg}")
  }

  def plan(n: Node): Seq[Instruction] = {
    var seenTables = Seq[String]()
    def helper(n: Node): Seq[Instruction] = n match {
      case RefreshNode(table) => {
        seenTables +:= table
        return Seq(this.instructionSet(table)("refresh"))
      }
      case HashRefreshNode(table) => {
        seenTables +:= table
        return Seq(this.instructionSet(table)("hash refresh"))
      }
      case AllNode(nodes) => {
        return nodes.flatMap(helper)
      }
    }

    return helper(n) ++ this.dag.plan(seenTables)
  }

  def execute(spark: SparkSession, instructions: Seq[Instruction], dss: DeltaTypes.Datasets): DeltaTypes.Datasets = {
    return instructions.foldLeft(dss)((p, c) => c.execute(spark, p))
  }

  def run(arg: String, spark: SparkSession, dss: DeltaTypes.Datasets): DeltaTypes.Datasets = {
    val command = this.parse(arg)
    val instructions = this.plan(command) 
    return this.execute(spark, instructions, dss)
  }
}

object DeltaState {
  type Kind = Byte
  val created: Kind = 0
  val updated: Kind = 1
  val neutral: Kind = 2
  val deleted: Kind = 3
  val column = "__delta_state_kind"
}

object Udfs {
  private val innerJoinStateMap = Array[DeltaState.Kind](0,0,0,3,0,1,1,3,0,1,2,3,3,3,3,3)
  private val leftJoinStateMap = Array[DeltaState.Kind](0,0,0,3,0,1,1,3,0,1,2,3,3,3,3,3)
  val innerJoinStateUdf = udf((x: DeltaState.Kind, y: DeltaState.Kind) => this.innerJoinStateMap(x * 4 + y))
  val leftJoinStateUdf = udf((x: DeltaState.Kind, y: DeltaState.Kind) => this.leftJoinStateMap(x * 4 + y))
}

object Ops {
  def join(spark: SparkSession, lds: DataFrame, rds: DataFrame, on: Column, kind: String): DataFrame = {
    import spark.implicits._

    val ldc = s"l\${DeltaState.column}"
    val rdc = s"r\${DeltaState.column}"

    val ds0 = lds.withColumnRenamed(DeltaState.column, ldc)
    val ds1 = rds.withColumnRenamed(DeltaState.column, rdc)

    val ds = kind match {
      case "inner" => ds0.join(ds1, on, "inner").withColumn(DeltaState.column, Udfs.innerJoinStateUdf(col(ldc), col(rdc)))
      case "left" => ds0.join(ds1, on, "left").withColumn(DeltaState.column, Udfs.leftJoinStateUdf(col(ldc), col(rdc)))
      case "right" => ds0.join(ds1, on, "right").withColumn(DeltaState.column, Udfs.leftJoinStateUdf(col(rdc), col(ldc)))
      case _ => throw new Exception(s"bad kind: \${kind} in join op")
    }

    return ds.drop(ldc, rdc)
  }
}
`;
}