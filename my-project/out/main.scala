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

  case class Table3(
    id: String,
    firstName: String,
    lastName: String,
    xid: String,
    tid: String,
    value: Int,
    fullName: String,
  )
}

object DeltaTypes {
  type Datasets = (Dataset[Table1], Dataset[Table2], Dataset[Table3])

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

  case class Table3(
    id: String,
    firstName: String,
    lastName: String,
    xid: String,
    tid: String,
    value: Int,
    fullName: String,
    __delta_state: DeltaState.Kind,
  )
}

object Storages {
}

object Instructions {
  object RefreshTable1Instruction extends Instruction {
    def execute(spark: SparkSession, dss: DeltaTypes.Datasets): DeltaTypes.Datasets = {
      import spark.implicits._

      val ds = Storages
        .Table1Storage
        .read(spark)
        .withColumn(DeltaState.deltaColumn, lit(DeltaState.created))
        .as[DeltaTypes.Table1]

      return (ds, dss._2, dss._3)
    }
  }

  object FetchTable1Instruction extends Instruction {
    def execute(spark: SparkSession, dss: DeltaTypes.Datasets): DeltaTypes.Datasets = {
      import spark.implicits._

      val ds = Storages
        .Table1Storage
        .read(spark)
        .withColumn(DeltaState.deltaColumn, lit(DeltaState.neutral))
        .as[DeltaTypes.Table1]

      return (ds, dss._2, dss._3)
    }
  }

  object RefreshTable2Instruction extends Instruction {
    def execute(spark: SparkSession, dss: DeltaTypes.Datasets): DeltaTypes.Datasets = {
      import spark.implicits._

      val ds = Storages
        .Table2Storage
        .read(spark)
        .withColumn(DeltaState.deltaColumn, lit(DeltaState.created))
        .as[DeltaTypes.Table2]

      return (dss._1, ds, dss._3)
    }
  }

  object FetchTable2Instruction extends Instruction {
    def execute(spark: SparkSession, dss: DeltaTypes.Datasets): DeltaTypes.Datasets = {
      import spark.implicits._

      val ds = Storages
        .Table2Storage
        .read(spark)
        .withColumn(DeltaState.deltaColumn, lit(DeltaState.neutral))
        .as[DeltaTypes.Table2]

      return (dss._1, ds, dss._3)
    }
  }

  object FetchTable3Instruction extends Instruction {
    def execute(spark: SparkSession, dss: DeltaTypes.Datasets): DeltaTypes.Datasets = {
      import spark.implicits._

      val ds = Storages
        .Table3Storage
        .read(spark)
        .withColumn(DeltaState.deltaColumn, lit(DeltaState.neutral))
        .as[DeltaTypes.Table3]

      return (dss._1, dss._2, ds)
    }
  }

  object DeriveTable3Instruction extends Instruction {
    def execute(spark: SparkSession, dss: DeltaTypes.Datasets): DeltaTypes.Datasets = {
      import spark.implicits._

      return (dss._1, dss._2, ds)
    }
  }

  object StoreTable3Instruction extends Instruction {
    def execute(spark: SparkSession, dss: DeltaTypes.Datasets): DeltaTypes.Datasets = {
      import spark.implicits._

      val ds = Storages
        .Table3Storage
        .write(spark, dss._3)

      return dss
    }
  }
}

object my-project {
  private val instructionSet = Map[String, Map[String, Instruction[DeltaTypes.Datasets]]](
    ("Table1" -> Map(
      ("refresh" -> Instructions.RefreshTable1Instruction),
      ("fetch" -> Instructions.FetchTable1Instruction),
    )),
    ("Table2" -> Map(
      ("refresh" -> Instructions.RefreshTable2Instruction),
      ("fetch" -> Instructions.FetchTable2Instruction),
    )),
    ("Table3" -> Map(
      ("fetch" -> Instructions.FetchTable3Instruction),
      ("derive" -> Instructions.DeriveTable3Instruction),
      ("store" -> Instructions.StoreTable3Instruction),
    )),
  )

  private val vertices = Map[String, Map[String, Vertex]](
    ("Table1" -> new Vertex("Table1", Seq(), Seq(), true)),
    ("Table2" -> new Vertex("Table2", Seq(), Seq(), true)),
    ("Table3" -> new Vertex("Table3", Seq(), Seq(), true)),
  )

  private val dag = new Dag[DeltaTypes.Datasets](
    this.vertices,
    this.instructionSet,
  )

  private val interpreter = new Interpreter[DeltaTypes.Datasets](
    this.dag,
    this.instructionSet,
  )

  def main(args: Array[String]): Unit = {
  }
}