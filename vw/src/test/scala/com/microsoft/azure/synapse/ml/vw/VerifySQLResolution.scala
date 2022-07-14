package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.base.SparkSessionFactory
import com.microsoft.azure.synapse.ml.core.test.benchmarks.Benchmarks
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions => F, types => T}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier, analysis}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.JoinReorderDP.JoinPlan
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.FunSuite


case class AutoJoinResolutionRule(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    println(s"resolution rule exec: ${plan}")

    plan match {
      case p: Project => {
        val newList = p.projectList.map { attr => attr match {
            case unresolved: UnresolvedAttribute => {
              Some(unresolved)
            }
            case n: NamedExpression => None
          }
        }.flatten

        if (newList.isEmpty)
          p
        else {
          println("INJECTING B")
          val attrPK = UnresolvedAttribute(Seq("B", "PK"))

          val missingTable = Project(
            newList :+ attrPK,
            UnresolvedRelation(Seq("global_temp", "B"))
          )

          val injectedJoin = Join(
            left = p.child,
            right = missingTable,
            joinType = Inner,
            condition = Some(EqualTo(
              p.child.allAttributes.attrs.filter { p => p.name == "FK" }.head,
              attrPK
            )),
            JoinHint.NONE
          )

          Project(p.projectList, injectedJoin)
        }
      }
      case n: LogicalPlan => n
    }
  }
}

//case class AutoJoinParser(session: SparkSession, parser: ParserInterface) extends ParserInterface {
//  override def parsePlan(sqlText: String): LogicalPlan = {
//    val plan = parser.parsePlan(sqlText)
//
//    plan
//  }
//
//  override def parseExpression(sqlText: String): Expression = parser.parseExpression(sqlText)
//
//  override def parseTableIdentifier(sqlText: String): TableIdentifier = parser.parseTableIdentifier(sqlText)
//
//  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = parser.parseFunctionIdentifier(sqlText)
//
//  override def parseMultipartIdentifier(sqlText: String): Seq[String] = parser.parseMultipartIdentifier(sqlText)
//
//  override def parseTableSchema(sqlText: String): StructType = parser.parseTableSchema(sqlText)
//
//  override def parseDataType(sqlText: String): DataType = parser.parseDataType(sqlText)
//}

class VerifySQLResolution extends FunSuite {
//  lazy val moduleName = "vw"
//  val numPartitions = 1

  test ("Rules injection") {

    val conf = new SparkConf()
      .setAppName("SQL Injection")
      .setMaster(s"local[1]")
      .set("spark.logConf", "true")
      .set("spark.sql.shuffle.partitions", "20")
      .set("spark.driver.maxResultSize", "6g")
      // .set("spark.sql.warehouse.dir", SparkSessionFactory.LocalWarehousePath)
      .set("spark.sql.crossJoin.enabled", "true")

    val sess = SparkSession.builder()
      .config(conf)
      .withExtensions( { extensions => {
        extensions.injectResolutionRule { session => AutoJoinResolutionRule(session) }
//        extensions.injectParser { case (session, builder) => AutoJoinParser(session, builder) }
      }
      })
      .getOrCreate()

    val spark = sess.sqlContext
    //sess.sparkContext.setLogLevel(logLevel)

    import spark.implicits._

    val dfA = Seq(
      (1, 100),
      (1, 200),
      (2, 300),
    ).toDF("FK", "x")

    dfA.explain()

    val dfB = Seq(
      (1, 5),
      (2, 10),
    ).toDF("PK", "y")

    dfA.createOrReplaceGlobalTempView("A")
    dfB.createOrReplaceGlobalTempView("B")

    spark.sql("SELECT x, B.y FROM global_temp.A").show()
    // spark.sql("SELECT x, b.y FROM global_temp.A JOIN global_temp.B ON FK = PK").show()
  }
}
