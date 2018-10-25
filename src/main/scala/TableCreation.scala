import com.amazonaws.services.dynamodbv2.model.{DeleteItemRequest, DeleteTableRequest, ListTablesRequest}
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.gu.scanamo.Scanamo
import com.gu.scanamo.error.DynamoReadError
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object TableCreation extends StudentApi with App{

  val logger: Logger = LoggerFactory.getLogger(this.getClass())

  //create table
  LocalDynamoDB.createTable(LocalDynamoDB.client)("student")('name -> S).getTableDescription

  //get list of table Request
  val list = new ListTablesRequest()
  LocalDynamoDB.client.listTables(list).getTableNames.toArray.toList.foreach(n => logger.info("name: " + n))

  //data insert
  val t: List[Student] = (0 to 1000).map{ i =>
    Student(1, "hemanth" + i)
  }.toList

  val ops = for{
    _ <- student.putAll(t.toSet)
    stds <- student.scan()
  } yield stds

  val res: List[Either[DynamoReadError, Student]] = Scanamo.exec(LocalDynamoDB.client)(ops)

  import cats.syntax.either._
  logger.info(s"${res.flatMap(_.toOption).length}")

  //Delete table Request
  val request = new DeleteTableRequest().withTableName("student")
  LocalDynamoDB.client.deleteTable(request)

  logger.info("total no. of tables" + LocalDynamoDB.client.listTables(list).getTableNames.toArray.toList.length)
}

