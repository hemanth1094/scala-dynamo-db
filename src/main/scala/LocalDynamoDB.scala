import java.util

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.gu.scanamo.Scanamo
import com.gu.scanamo.ops.ScanamoOps

import scala.collection.JavaConverters._

trait LocalDynamoDB {
  private val maxErrorRetry = 3
  private val timeout = 5 * 1000 //in milliseconds
  private val config = new ClientConfiguration()
  config.setConnectionTimeout(timeout)
  config.setMaxErrorRetry(maxErrorRetry)

  val clientBuilder = AmazonDynamoDBAsyncClient.asyncBuilder()

  val client: AmazonDynamoDBAsync

  def createTableWithIndex(
                            tableName: String,
                            secondaryIndexName: String,
                            primaryIndexAttributes: List[(Symbol, ScalarAttributeType)],
                            secondaryIndexAttributes: List[(Symbol, ScalarAttributeType)]
                          ): CreateTableResult =
    client.createTable(
      new CreateTableRequest()
        .withTableName(tableName)
        .withAttributeDefinitions(
          attributeDefinitions(primaryIndexAttributes ++ (secondaryIndexAttributes diff primaryIndexAttributes))
        )
        .withKeySchema(keySchema(primaryIndexAttributes))
        .withProvisionedThroughput(arbitraryThroughputThatIsIgnoredByDynamoDBLocal)
        .withGlobalSecondaryIndexes(
          new GlobalSecondaryIndex()
            .withIndexName(secondaryIndexName)
            .withKeySchema(keySchema(secondaryIndexAttributes))
            .withProvisionedThroughput(arbitraryThroughputThatIsIgnoredByDynamoDBLocal)
            .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
        )
    )

  def createTable(tableName: String)(attributes: (Symbol, ScalarAttributeType)*): CreateTableResult =
    client.createTable(
      attributeDefinitions(attributes),
      tableName,
      keySchema(attributes),
      arbitraryThroughputThatIsIgnoredByDynamoDBLocal
    )

  def deleteTable(tableName: String): DeleteTableResult =
    client.deleteTable(tableName)

  def withTable[T](tableName: String)(attributeDefinitions: (Symbol, ScalarAttributeType)*)(
    thunk: => T
  ): T = {
    createTable(tableName)(attributeDefinitions: _*)
    val res = try {
      thunk
    } finally {
      client.deleteTable(tableName)
      ()
    }
    res
  }

  def withRandomTable[T](attributeDefinitions: (Symbol, ScalarAttributeType)*)(
    thunk: String => T
  ): T = {
    var created: Boolean = false
    var tableName: String = null
    while (!created) {
      try {
        tableName = java.util.UUID.randomUUID.toString
        createTable(tableName)(attributeDefinitions: _*)
        created = true
      } catch {
        case e: ResourceInUseException =>
      }
    }

    val res = try {
      thunk(tableName)
    } finally {
      client.deleteTable(tableName)
      ()
    }
    res
  }

  def usingTable[T](tableName: String)(attributeDefinitions: (Symbol, ScalarAttributeType)*)(
    thunk: => T
  ): Unit = {
    withTable(tableName)(attributeDefinitions: _*)(thunk)
    ()
  }

  def usingRandomTable[T](attributeDefinitions: (Symbol, ScalarAttributeType)*)(
    thunk: String => T
  ): Unit = {
    withRandomTable(attributeDefinitions: _*)(thunk)
    ()
  }

  def withTableWithSecondaryIndex[T](tableName: String, secondaryIndexName: String)(
    primaryIndexAttributes: (Symbol, ScalarAttributeType)*
  )(secondaryIndexAttributes: (Symbol, ScalarAttributeType)*)(
                                      thunk: => T
                                    ): T = {
    createTableWithIndex(
      tableName,
      secondaryIndexName,
      primaryIndexAttributes.toList,
      secondaryIndexAttributes.toList
    )
    val res = try {
      thunk
    } finally {
      client.deleteTable(tableName)
      ()
    }
    res
  }

  def withRandomTableWithSecondaryIndex[T](primaryIndexAttributes: (Symbol, ScalarAttributeType)*)(secondaryIndexAttributes: (Symbol, ScalarAttributeType)*)(
    thunk: (String, String) => T
  ): T = {
    var tableName: String = null
    var indexName: String = null
    var created: Boolean = false
    while (!created) {
      try {
        tableName = java.util.UUID.randomUUID.toString
        indexName = java.util.UUID.randomUUID.toString
        createTableWithIndex(
          tableName,
          indexName,
          primaryIndexAttributes.toList,
          secondaryIndexAttributes.toList
        )
        created = true
      } catch {
        case t: ResourceInUseException =>
      }
    }

    val res = try {
      thunk(tableName, indexName)
    } finally {
      client.deleteTable(tableName)
      ()
    }
    res
  }

  private def keySchema(attributes: Seq[(Symbol, ScalarAttributeType)]): util.List[KeySchemaElement] = {
    val hashKeyWithType :: rangeKeyWithType = attributes.toList
    val keySchemas = hashKeyWithType._1 -> KeyType.HASH :: rangeKeyWithType.map(_._1 -> KeyType.RANGE)
    keySchemas.map { case (symbol, keyType) => new KeySchemaElement(symbol.name, keyType) }.asJava
  }

  private def attributeDefinitions(attributes: Seq[(Symbol, ScalarAttributeType)]): util.List[AttributeDefinition] =
    attributes.map { case (symbol, attributeType) => new AttributeDefinition(symbol.name, attributeType) }.asJava

  def getTables: List[String] = {
    val request = new ListTablesRequest()
    client.listTables(request).getTableNames.toArray.toList.map(_.toString)
  }

  def execute[T](ops: ScanamoOps[T]): T = Scanamo.exec(client)(ops)

  private val arbitraryThroughputThatIsIgnoredByDynamoDBLocal = new ProvisionedThroughput(40000L, 40000L)
}

object LocalDynamoDB extends LocalDynamoDB {
  val client: AmazonDynamoDBAsync =
    clientBuilder
      .withCredentials(new DefaultAWSCredentialsProviderChain)
      .withEndpointConfiguration(new EndpointConfiguration("http://localhost:8000", "ap-south-1"))
      .build()
}