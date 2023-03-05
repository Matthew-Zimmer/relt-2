
export interface ScalaTemplateOptions {
  packageName: string;
  names: string[];
  allNames: string[];
  types: string[];
  deltaTypes: string[];
  handlers: string[];
}

export const scalaTemplate = (options: ScalaTemplateOptions) => `\
package ${options.packageName}

// --- Standard Libraries ---
import scala.reflect.runtime.universe.{ TypeTag }
import org.apache.spark.sql.{ Encoders, Dataset, Row, SparkSession, Column }
import org.apache.spark.sql.functions.{ collect_list, struct, sum, lit, udf, col }
import java.sql.Date
import scala.util.control._
import scala.collection.mutable.LinkedHashMap

// --- Runtime ---

object DeltaState {
  type Kind = Byte
  val created: Kind = 0
  val updated: Kind = 1
  val neutral: Kind = 2
  val deleted: Kind = 3
  val column = "__delta_state_kind"
}

trait DeltaStateTable {
  val __delta_state_kind: DeltaState.Kind
}

trait Storage[T] {
  def read(spark: SparkSession): Dataset[T];
  def insert(spark: SparkSession, ds: Dataset[T]): Unit;
  def update(spark: SparkSession, ds: Dataset[T]): Unit;
  def delete(spark: SparkSession, ds: Dataset[T]): Unit;
  def clear(spark: SparkSession): Unit;
}

object StorageUtils {
  def write[S <: Product : TypeTag, D](spark: SparkSession, storage: Storage[S], ds: Dataset[D]): Unit = {
    import spark.implicits._

    storage.insert(spark, ds.where(col(DeltaState.column) === DeltaState.created).drop(DeltaState.column).as[S]);
    storage.update(spark, ds.where(col(DeltaState.column) === DeltaState.updated).drop(DeltaState.column).as[S]);
    storage.delete(spark, ds.where(col(DeltaState.column) === DeltaState.deleted).drop(DeltaState.column).as[S]);
  }
}

class DeltaFileStorage[T <: Product : TypeTag](private val loc: String) extends Storage[T] {
  def read(spark: SparkSession): Dataset[T] = {
    import spark.implicits._

    val ds = spark.emptyDataset[T]
    return ds
  }

  def insert(spark: SparkSession, ds: Dataset[T]): Unit = {

  }

  def update(spark: SparkSession, ds: Dataset[T]): Unit = {

  }

  def delete(spark: SparkSession, ds: Dataset[T]): Unit = {

  }

  def clear(spark: SparkSession): Unit = {

  }
}

class IndexerStorage[T <: Product : TypeTag](private val loc: String, private val column: String) extends Storage[T] {
  def read(spark: SparkSession): Dataset[T] = {
    import spark.implicits._

    val ds = spark.emptyDataset[T]
    return ds
  }

  def insert(spark: SparkSession, ds: Dataset[T]): Unit = {

  }

  def update(spark: SparkSession, ds: Dataset[T]): Unit = {

  }

  def delete(spark: SparkSession, ds: Dataset[T]): Unit = {

  }

  def clear(spark: SparkSession): Unit = {

  }
}

class PostgresTableStorage[T <: Product : TypeTag](private val loc: String) extends Storage[T] {
  def read(spark: SparkSession): Dataset[T] = {
    import spark.implicits._

    val ds = spark.emptyDataset[T]
    return ds
  }

  def insert(spark: SparkSession, ds: Dataset[T]): Unit = {

  }

  def update(spark: SparkSession, ds: Dataset[T]): Unit = {

  }

  def delete(spark: SparkSession, ds: Dataset[T]): Unit = {

  }

  def clear(spark: SparkSession): Unit = {

  }
}

// case class DatabaseStorage() extends Storage

// --- Runtime:Plans ---

trait Plan[DSS, -DS <: Product] {
  def execute(spark: SparkSession, dss: DSS): DSS;
}

// This plan represents when a storage is completely reset
// reads the data from storage and marks it as created
class RefreshPlan[DSS, DS <: Product : TypeTag, S <: Product : TypeTag](
  private val storage: Storage[S],
  private val combine: (DSS, Dataset[DS]) => DSS,
) extends Plan[DSS, DS] {
  def execute(spark: SparkSession, dss: DSS): DSS = {
    import spark.implicits._

    val ds = this.storage
      .read(spark)
      .withColumn(DeltaState.column, lit(DeltaState.created))
      .as[DS]
    
    return combine(dss, ds)
  }
}

// This plan represents when data which is needed from storage
// reads the data from storage and marks it as neutral
class FetchPlan[DSS, DS <: Product : TypeTag, S <: Product : TypeTag](
  private val storage: Storage[S],
  private val combine: (DSS, Dataset[DS]) => DSS,
) extends Plan[DSS, DS] {
  def execute(spark: SparkSession, dss: DSS): DSS = {
    import spark.implicits._

    val ds = this.storage
      .read(spark)
      .withColumn(DeltaState.column, lit(DeltaState.neutral))
      .as[DS]
    
    return combine(dss, ds)
  }
}

// This plan represents derived data which is needs to be stored
// derives the data and then stores it
class StorePlan[DSS, DS <: Product : TypeTag, S <: Product : TypeTag](
  private val storage: Storage[S],
  private val create: (SparkSession, DSS) => Dataset[DS], 
  private val combine: (DSS, Dataset[DS]) => DSS,
) extends Plan[DSS, DS] {
  def execute(spark: SparkSession, dss: DSS): DSS = {
    import spark.implicits._

    val ds = this.create(spark, dss)
    StorageUtils.write(spark, this.storage, ds)

    return combine(dss, ds)
  }
}

// This plan represents derived data
// derives the data
class DerivePlan[DSS, DS <: Product : TypeTag](
  private val create: (SparkSession, DSS) => Dataset[DS], 
  private val combine: (DSS, Dataset[DS]) => DSS,
) extends Plan[DSS, DS] {
  def execute(spark: SparkSession, dss: DSS): DSS = {
    import spark.implicits._

    val ds = this.create(spark, dss)

    return this.combine(dss, ds)
  }
}

// This plan represents data in storage which needs to be completed deleted
// deletes all data from storage
class InvalidatePlan[DSS, DS <: Product : TypeTag, S <: Product : TypeTag](
  private val storage: Storage[S],
) extends Plan[DSS, DS] {
  def execute(spark: SparkSession, dss: DSS): DSS = {
    this.storage.clear(spark)
    return dss
  }
}

trait Handler[DSS] {
  val name: String;
  val consumes: Array[String];
  val feeds: Array[String];
  val hasCache: Boolean;
  val plans: Map[String, Plan[DSS, Nothing]];
}

class Planner[DSS](
  private val createInitialDss: SparkSession => DSS,
  private val handlers: Map[String, Handler[DSS]],
) {
  type PPlan = Plan[DSS, Nothing]
  type PPlans = Seq[PPlan]
  type PMPlans = Map[String, PPlan]

  def execute(spark: SparkSession, args: Array[String]): Unit = {
    var dss = this.createInitialDss(spark)
    val plans = this.preparePlans(args)
    plans.foreach(plan => dss = plan.execute(spark, dss))
  }

  def preparePlans(args: Array[String]): PPlans = {
    var plans = Seq[PPlan]()
    val sourceTypes = this.handlers.values.filter(x => x.consumes.length == 0).toSeq
    var i = 0

    while (i < args.length) {
      val arg = args(i)
      if (arg == "refresh") {
        i += 1
        val arg1 = if (i < args.length) { args(i) } else throw new Exception("")
        val sourceType = sourceTypes.filter(x => x.name == arg1)
        if (sourceType.length != 1)
          throw new Exception(s"Unknown source type: \${arg1}")
        i += 1
        val handler = sourceType(0)
        plans = this.generateRefreshPlansFor(handler, handler.name, Seq())
      }
      else {
        throw new Exception(s"Unknown Command: \${arg}")
      }
    }

    return plans
  }

  def generateRefreshPlansFor(handler: Handler[DSS], root: String, seen: Seq[String]): PPlans = {
    var plans: PPlans = Seq()
    if (seen contains handler.name) return plans

    if (handler.name == root)
      plans +:= handler.plans("RefetchPlan")
    else if (handler.consumes.length == 0)
      plans +:= handler.plans("FetchPlan")
    else if (handler.hasCache)
      plans ++:= Seq(handler.plans("InvalidatePlan"), handler.plans("StorePlan"))
    else 
      plans +:= handler.plans("DerivePlan")

    val seenMe = seen :+ handler.name

    plans ++:= handler.consumes.flatMap(
      x => this.generateRefreshPlansFor(this.handlers(x), root, seenMe)
    )

    plans ++:= handler.feeds.flatMap(
      x => this.generateRefreshPlansFor(this.handlers(x), root, seenMe)
    )

    return plans
  }

}

// --- Generated Section ---

// --- Types ---

object Types {
${options.types.map(x => x.split('\n').map(x => `  ${x}`).join('\n')).join('\n')}
}

// --- Delta Types ---

object DeltaTypes {
  type Datasets = (${options.names.map(x => `Dataset[${x}]`).join(',')})

${options.deltaTypes.map(x => x.split('\n').map(x => `  ${x}`).join('\n')).join('\n')}
}


// --- Handlers ---

object Handlers {
${options.handlers.map(x => x.split('\n').map(x => `  ${x}`).join('\n')).join('\n')}
}

// --- Job Section

object ${options.packageName}Job {
  // --- CONSTANT SECTION ---

  private val planner = new Planner[DeltaTypes.Datasets](
    spark => {
      import spark.implicits._
      (
${options.names.map(x => `        spark.emptyDataset[DeltaTypes.${x}],`).join('')} 
      )
    },
    Map(
${options.allNames.map(x => `        "${x}" -> Handlers.${x},\n`).join('')})
  )

  // --- JOB SECTION ---
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("${options.packageName}")
      .master("local") // LOCAL
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    this.planner.execute(spark, args)
  }
}
`;