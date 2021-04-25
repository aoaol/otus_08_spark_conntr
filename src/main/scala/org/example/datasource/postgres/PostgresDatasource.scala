package org.example.datasource.postgres

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.{DriverManager, ResultSet}
import java.util
import scala.collection.JavaConverters._


class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = PostgresTable.schema

  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): Table = new PostgresTable(properties.get("tableName")) // TODO: Error handling
}

class PostgresTable(val name: String) extends SupportsRead with SupportsWrite {
  override def schema(): StructType = PostgresTable.schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new PostgresScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new PostgresWriteBuilder(info.options)
}

object PostgresTable {
  val schema: StructType = new StructType().add("user_id", LongType)
}

/** Read */
case class PgReadOptions( url: String, user: String, password: String, tableName: String, partCount: Int, orderByField: String)

class PostgresScanBuilder( options: CaseInsensitiveStringMap) extends ScanBuilder {
  override def build(): Scan = new PostgresScan( PgReadOptions(
    options.get("url"), options.get("user"), options.get("password"), options.get("tableName"), options.get("partCount").toInt, options.get("orderByField")
  ))
}

class PostgresPartition (val offset: Long, val limit: Long) extends InputPartition

class PostgresScan( options: PgReadOptions) extends Scan with Batch {
  override def readSchema(): StructType = PostgresTable.schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {

    val connection = DriverManager.getConnection( options.url, options.user, options.password )
    val statement = connection.prepareStatement(s"select count(*) cnt from ${options.tableName}")
    val resultSet = statement.executeQuery()
    resultSet.next()
    val cnt = resultSet.getInt("cnt")
    connection.close()

    val partCount = options.partCount
    val delta = if (cnt % partCount == 0) 0 else 1
    val step = (cnt / partCount) + delta

    val ret = (for (offs <- 0 to cnt - 1 + delta   by step) yield new PostgresPartition( offs, step )).toArray

    for (e <- ret)
      println(s"^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ #&*&*&*&*&*&*&*&**&*&*&*&*&*&*&*&*&*&   InputPartition:  ${e.asInstanceOf[ PostgresPartition ].offset}, ${e.asInstanceOf[ PostgresPartition ].limit}")

    ret.asInstanceOf[ Array[InputPartition] ]
  }

  override def createReaderFactory(): PartitionReaderFactory = new PostgresPartitionReaderFactory( options )
}

class PostgresPartitionReaderFactory( options: PgReadOptions)
  extends PartitionReaderFactory {
  override def createReader( partition: InputPartition): PartitionReader[InternalRow] = new PostgresPartitionReader( options, partition)
}

class PostgresPartitionReader( options: PgReadOptions, partition: InputPartition) extends PartitionReader[InternalRow] {

  private val connection = DriverManager.getConnection( options.url, options.user, options.password )
  private val statement = connection.createStatement()
  val orderByFieldVlause = if (options.orderByField.nonEmpty) s"order by ${options.orderByField}"  else  ""
  val offset = partition.asInstanceOf[ PostgresPartition ].offset
  val limit = partition.asInstanceOf[ PostgresPartition ].limit
  val query = s"select * from ${options.tableName} $orderByFieldVlause  offset $offset  limit $limit"
  println( s" | / | / | | / | / | | / | / | | / | / | | / | / | | / | / | | / $query")

  private val resultSet = statement.executeQuery( query )

  override def next(): Boolean = resultSet.next()

  override def get(): InternalRow = InternalRow(resultSet.getLong(1))

  override def close(): Unit = connection.close()
}

/** Write */
case class ConnectionProperties(url: String, user: String, password: String, tableName: String)


class PostgresWriteBuilder(options: CaseInsensitiveStringMap) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new PostgresBatchWrite(ConnectionProperties(
    options.get("url"), options.get("user"), options.get("password"), options.get("tableName")
  ))
}

class PostgresBatchWrite(connectionProperties: ConnectionProperties) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory =
    new PostgresDataWriterFactory(connectionProperties)

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class PostgresDataWriterFactory(connectionProperties: ConnectionProperties) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = new PostgresWriter( connectionProperties )
}

object WriteSucceeded extends WriterCommitMessage

class PostgresWriter(connectionProperties: ConnectionProperties) extends DataWriter[InternalRow] {

  val connection = DriverManager.getConnection(
    connectionProperties.url,
    connectionProperties.user,
    connectionProperties.password
  )
  val statement = "insert into users (user_id) values (?)"
  val preparedStatement = connection.prepareStatement(statement)

  override def write(record: InternalRow): Unit = {
    val value = record.getLong(0)
    preparedStatement.setLong(1, value)
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = connection.close()
}

