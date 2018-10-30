import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.amazonaws.services.dynamodbv2.model.{BatchWriteItemResult, DeleteItemResult}
import com.gu.scanamo.Table
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.syntax._
import org.slf4j.{Logger, LoggerFactory}

trait ContentFileInfoRepo {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def error(msg: String, ex: Throwable) = logger.error(msg, ex)

  val dynamoDB: LocalDynamoDB

  def contentFileInfo(tableName: String): Table[ContentFileInfo] = Table[ContentFileInfo](tableName)

  def insertAll(tableName: String, data: List[ContentFileInfo]): List[BatchWriteItemResult] = {
    val request = contentFileInfo(tableName).putAll(data.toSet)
    dynamoDB.execute(request)
  }

  def insert(tableName: String, content: ContentFileInfo): Boolean = {
    val request = contentFileInfo(tableName).put(content)
    val result = dynamoDB.execute(request)
    result match {
      case Some(res) => res.isRight
      case None => false
    }
  }

  def delete(tableName: String): Boolean =
    try {
      dynamoDB.deleteTable(tableName).getTableDescription.getTableName == tableName
    } catch {
      case ex =>
        error(s"Got error while deleting the table $tableName", ex)
        false
    }

  def getContents(tableName: String): List[Either[DynamoReadError, ContentFileInfo]] = {
    val req = contentFileInfo(tableName).scan()
    dynamoDB.execute(req)
  }

  def getContentById(tableName: String, id: String): Option[ContentFileInfo] = {
    val req = contentFileInfo(tableName).get('id -> id)
    val result = dynamoDB.execute(req)
    result match {
      case Some(res) => if (res.isRight) res.right.toOption else None
      case None => None
    }
  }

  def deleteById(tableName: String, id: String): DeleteItemResult = {
    val req: ScanamoOps[DeleteItemResult] = contentFileInfo(tableName).delete('id -> id)
    val result = dynamoDB.execute(req)
    result
  }

  def createTable(tableName: String): Boolean = try {
    dynamoDB.createTable(tableName)('id -> S).getTableDescription.getTableStatus == tableName
  } catch {
    case ex =>
      error(s"Got error while creating the table $tableName", ex)
      false
  }
}

class ContentFileInfoApi extends ContentFileInfoRepo {

  val dynamoDB: LocalDynamoDB = LocalDynamoDB

}

case class ContentFileInfo(id: String, file: String)
