import java.io.{BufferedInputStream, File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.charset.{Charset, CodingErrorAction}
import java.nio.file.Files

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.gu.scanamo.Scanamo
import com.gu.scanamo.syntax._
import org.mockito.Matchers.any
import org.slf4j.{Logger, LoggerFactory}

import scala.io.{Codec, Source}

object TableCreation extends App {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val contentFileInfoRepo = new ContentFileInfoApi

  //create table
  contentFileInfoRepo.createTable("content")

  //get list of table Request
  val list = new ListTablesRequest()
  LocalDynamoDB.client.listTables(list).getTableNames.toArray.toList.foreach(n => logger.info("name: " + n))

  val table = contentFileInfoRepo.contentFileInfo("content")

  val scan = table.scan()
  val req = new QueryRequest("content")
  logger.info("count before insert: " + contentFileInfoRepo.getContents("content").length)

  val localTextFilePath = "src/main/resources/sample.txt"
  val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
  val source = Source.fromFile(localTextFilePath)(codec).getLines().mkString(" ")
    .replaceAll("\n", " ").replaceAll("\\s\\s+", " ")

  val t: List[ContentFileInfo] = (0 to 0).map { i =>
    ContentFileInfo(s"${i}1234-1232-1232-12323-12323", source)
  }.toList

  logger.info("inserting data....")
  contentFileInfoRepo.insertAll("content", t)

  logger.info("count after insert: " + contentFileInfoRepo.getContents("content").length)


}

