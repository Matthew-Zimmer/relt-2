

// Library

trait Instruction[T] {
  def execute(spark: SparkSession, dss: T): T
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
  def isSource(): Boolean = hasStorage && incoming.size === 0 
  def hasCache(): Boolean = hasStorage && incoming.size !== 0
}

class Dag[T](
  private val vertices: Map[String, Vertex],
  private val instructionSet: Map[String, Map[String, Instruction[T]]],
) {
  def plan(seenTables: Seq[String]): Seq[Instruction[T]] = {
    val vertices = this.topologicalOrdering(seenTables)
    return vertices.flatMap(this.instructionFor)
  }

  def instructionFor(v: Vertex): Seq[Instruction[T]] = {
    if (v.isSource) {
      return Seq(
        this.instructionSet["fetch"][v.name],
      )
    }
    else if (v.hasStorage) {
      return Seq(
        this.instructionSet["derive"][v.name],
        this.instructionSet["store"][v.name],
      )
    }
    else {
      return Seq(
        this.instructionSet["derive"][v.name],
      )
    }
  }

  private def topologicalOrdering(without: Seq[String]): Seq[String] = {
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
			
		return this.resolveMany(stack.filter(!(without contains _)))
  }

  private def resolve(name: String): Vertex = this.vertices[name]
  private def resolveMany(names: Seq[String]): Seq[Vertex] = names.map(this.resolve)
}

class Interpreter[T](
  private val dag: Dag[T], 
  private val instructionSet: Map[String, Map[String, Instruction[T]]]
) {
  def parse(arg: String): Node = {
    val clean = arg
      .replaceAll(" +", " ")
      .replaceAll("\n", " ")
      .replaceAll("\t", " ")
  }

  def plan(n: Node): Seq[Instruction[T]] = {
    var seenTables = Seq[String]()
    def helper(n: Node): Seq[Instruction[T]] = {
      var instructions = Seq[Instruction[T]]()

      return node match {
        case RefreshNode(table) => {
          seenTables +:= table
          instructions +:= this.instructionSet["refresh"][table]
        }
        case HashRefreshNode(table) => {
          seenTables +:= table
          instructions +:= this.instructionSet["hash refresh"][table]
        }
        case AllNode(nodes) => {
          instructions ++:= nodes.flatMap(helper)
        }
      }
    }

    return helper(n) ++ this.dag.plan(seenTables)
  }

  def execute(spark: SparkSession, dss: T): T = {
    return this.instructions.foldLeft((p, c) => c.execute(spark, p), dss)
  }

  def run(arg: String, spark: SparkSession, dss: T): T = {
    val command = this.interpreter.parse(args(0))
    val instructions = this.interpreter.plan(command) 
    return this.interpreter.execute(spark, instructions)
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
  def join[L, R, T <: Product : TypeTag](spark: SparkSession, lds: DataSet[L], rds: DataSet[R], on: Column, kind: String, drop: String): DataSet[T] = {
    import spark.implicits._

    val ldc = s"l${DeltaState.column}"
    val rdc = s"r${DeltaState.column}"

    val ds0 = lds.withColumnRenamed(DeltaState.column, ldc)
    val ds1 = rds.withColumnRenamed(DeltaState.column, rdc)

    val ds = kind match {
      case "inner" => ds0.join(ds1, on, "inner").withColumn(DeltaState.column, Udfs.innerJoinStateUdf(col(ldc), col(rdc)))
      case "left" => ds0.join(ds1, on, "left").withColumn(DeltaState.column, Udfs.leftJoinStateUdf(col(ldc), col(rdc)))
      case "right" => ds0.join(ds1, on, "right").withColumn(DeltaState.column, Udfs.leftJoinStateUdf(col(rdc), col(ldc)))
      case _ => throw new Exception(s"bad kind: ${kind} in join op")
    }

    return ds
      .drop(drop, ldc, rdc)
      .as[T]
  }
}

// Generated

object Types {
  case class Table1(
    id: String,
    firstName: String,
    lastName: String,
  )

  case class Table2(
    xid: String,
    tid: String,
    value: Int,
  )

  case class RELT_0(
    id: String,
    firstName: String,
    lastName: String,
    xid: String,
    value: Int,
  )

  case class Table3(
    id: String,
    firstName: String,
    lastName: String,
    xid: String,
    value: Int,
    fullName: String,
  )
}

object DeltaTypes {
  case class Table1(
    id: String,
    firstName: String,
    lastName: String,
    __delta_state: DeltaState.Kind,
  )

  case class Table2(
    xid: String,
    tid: String,
    value: Int,
    __delta_state: DeltaState.Kind,
  )

  case class RELT_0(
    id: String,
    firstName: String,
    lastName: String,
    xid: String,
    value: Int,
    __delta_state: DeltaState.Kind,
  )

  case class Table3(
    id: String,
    firstName: String,
    lastName: String,
    xid: String,
    value: Int,
    fullName: String,
    __delta_state: DeltaState.Kind,
  )
}

object Storages {
  object Table1Storage {
    def read(spark: SparkSession): Dataset[Types.Table1] = {
    }
  }

  object Table2Storage {
    def read(spark: SparkSession): Dataset[Types.Table2] = {
    }
  }

  object Table3Storage {
    def write(spark: SparkSession, Dataset[DeltaTypes.Table3]): Unit = {
    }
  }
}

object Instructions {
  object RefreshTable1Instruction extends Instruction {
    def execute(spark: SparkSession, dss: Datasets): Datasets = {
      import spark.implicits._
      
      val ds = Storages
        .Table1Storage
        .read(spark)
        .withColumn(DeltaState.column, DeltaState.created)
        .as[DeltaTypes.Table1]
      
      return (ds, dss._2, dss._3, dss._4)
    }
  }

  object HashRefreshTable1Instruction extends Instruction {
    def execute(spark: SparkSession, dss: Datasets): Datasets = {
      import spark.implicits._

      throw new Exception("TODO")
    }
  }

  object FetchTable1Instruction extends Instruction {
    def execute(spark: SparkSession, dss: Datasets): Datasets = {
      import spark.implicits._

      throw new Exception("TODO")
    }
  }

  object RefreshTable2Instruction extends Instruction {
    def execute(spark: SparkSession, dss: Datasets): Datasets = {
      import spark.implicits._
      
      val ds = Storages
        .Table2Storage
        .read(spark)
        .withColumn(DeltaState.column, DeltaState.created)
        .as[DeltaTypes.Table2]
      
      return (dss._1, ds, dss._3, dss._4)
    }
  }

  object FetchTable2Instruction extends Instruction {
    def execute(spark: SparkSession, dss: Datasets): Datasets = {
      import spark.implicits._

      throw new Exception("TODO")
    }
  }

  object DeriveRELT_0Instruction extends Instruction {
    def execute(spark: SparkSession, dss: Datasets): Datasets = {
      import spark.implicits._

      val ds = Ops.join[DeltaTypes.Table1, DeltaTypes.Table2, DeltaTypes.RELT_0](dss._1, dss._2, col("id") === col("tid"), "inner", "tid")

      return (dss._1, dss._2, ds, dss._4)
    }
  }

  object DeriveTable3Instruction extends Instruction {
    def execute(spark: SparkSession, dss: Datasets): Datasets = {
      import spark.implicits._
      
      val ds = dss
        ._3
        .withColumn("fullName", col("firstName") & lit(" ") & col("lastName"))
        .as[DeltaTypes.Table3]

      return (dss._1, dss._2, dss._3, ds)
    }
  }

  object StoreTable3Instruction extends Instruction {
    def execute(spark: SparkSession, dss: Datasets): Datasets = {
      import spark.implicits._

      Storages
        .Table3Storage
        .write(spark, dss._4)
      
      return dss
    }
  }
}

object MyProject {
  val instructionSet = Map[String, Map[String, Instruction[DeltaTypes.Datasets]]](
    "refresh" -> Map(
      "Table1" -> Instructions.RefreshTable1Instruction,
      "Table2" -> Instructions.RefreshTable2Instruction,
    ),
    "hash refresh" -> Map(
      "Table1" -> Instructions.HashRefreshTable1Instruction,
    ),
    "fetch" -> Map(
      "Table1" -> Instructions.FetchTable1Instruction,
      "Table2" -> Instructions.FetchTable2Instruction,
    ),
    "derive" -> Map(
      "RELT_0" -> Instructions.DeriveRELT_0Instruction,
      "Table3" -> Instructions.DeriveTable3Instruction,
    ),
    "store" -> Map(
      "Table3" -> Instructions.StoreTable3Instruction,
    ),
  )

  val vertices = Map[String, Vertex](
    "Table1" -> new Vertex("Table1", Seq(), Seq("RELT_0"), true),
    "Table2" -> new Vertex("Table2", Seq(), Seq("RELT_0"), true),
    "RELT_0" -> new Vertex("RELT_0", Seq("Table1", "Table2"), Seq("Table3"), false),
    "Table3" -> new Vertex("Table3", Seq("RELT_0"), Seq(), true),
  )

  val dag = new Dag[DeltaTypes.Datasets](
    this.vertices,
    this.instructionSet
  )

  val interpreter = new Interpreter[DeltaTypes.Datasets](
    this.dag,
    this.instructionSet,
  )

  def main(args: Array[String]): Unit = {
    if (args.size == 0) {
      println("Not enough args: missing command")
      exit(1)
    }
    
    val spark = 0
    val arg = args(0)
    var dss = (0, 0)

    dss = this.interpreter.run(arg, spark, dss)

  }
}
