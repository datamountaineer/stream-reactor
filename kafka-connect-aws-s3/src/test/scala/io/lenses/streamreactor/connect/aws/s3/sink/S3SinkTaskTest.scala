
/*
 * Copyright 2020 Lenses.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.lenses.streamreactor.connect.aws.s3.sink

import java.io.StringReader
import java.lang

import au.com.bytecode.opencsv.CSVReader
import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3SinkConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.formats.{AvroFormatReader, BytesFormatWriter, ParquetFormatReader}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3ProxyContext.{Credential, Identity}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.{S3ProxyContext, S3TestConfig, S3TestPayloadReader}
import org.apache.commons.io.IOUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.header.{ConnectHeaders, Header}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.jclouds.blobstore.options.ListContainerOptions
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class S3SinkTaskTest extends AnyFlatSpec with Matchers with S3TestConfig with MockitoSugar {

  import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._

  private val parquetFormatReader = new ParquetFormatReader()
  private val avroFormatReader = new AvroFormatReader()

  private val PrefixName = "streamReactorBackups"
  private val TopicName = "mytopic"

  private val DefaultProps = Map(
    AWS_REGION -> "eu-west-1",
    AWS_ACCESS_KEY -> Identity,
    AWS_SECRET_KEY -> Credential,
    AUTH_MODE -> AuthMode.Credentials.toString,
    CUSTOM_ENDPOINT -> S3ProxyContext.Uri,
    ENABLE_VIRTUAL_HOST_BUCKETS -> "true"
  )


  private val partitionedData: List[Struct] = List(
    new Struct(schema).put("name", "first").put("title", "primary").put("salary", null),
    new Struct(schema).put("name", "second").put("title", "secondary").put("salary", 100.00),
    new Struct(schema).put("name", "third").put("title", "primary").put("salary", 100.00),
    new Struct(schema).put("name", "first").put("title", null).put("salary", 200.00),
    new Struct(schema).put("name", "second").put("title", null).put("salary", 100.00),
    new Struct(schema).put("name", "third").put("title", null).put("salary", 100.00)
  )

  private val headerPartitionedRecords = partitionedData.zipWithIndex.map { case (user, k) =>
    new SinkRecord(TopicName, 1, null, null, schema, user, k, null, null, createHeaders(("headerPartitionKey", (k % 2).toString)))
  }

  private val records = users.zipWithIndex.map { case (user, k) =>
    new SinkRecord(TopicName, 1, null, null, schema, user, k)
  }

  private val keySchema = SchemaBuilder.struct()
    .field("phoneprefix", SchemaBuilder.string().required().build())
    .field("region", SchemaBuilder.int32().optional().build())
    .build()

  private val keyPartitionedRecords = List(
    new SinkRecord(TopicName, 1, null, createKey(keySchema, ("phoneprefix", "+44"), ("region", 8)), null, users(0), 0, null, null),
    new SinkRecord(TopicName, 1, null, createKey(keySchema, ("phoneprefix", "+49"), ("region", 5)), null, users(1), 1, null, null),
    new SinkRecord(TopicName, 1, null, createKey(keySchema, ("phoneprefix", "+49"), ("region", 5)), null, users(2), 2, null, null)
  )

  "S3SinkTask" should "flush on configured flush time intervals" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_INTERVAL = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)

    blobStoreContext.getBlobStore.list(BucketName,
      ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1")
    ).size() should be(0)

    Thread.sleep(1200) // wait for 1000 millisecond so the next call to put will cause a flush

    task.put(Seq().asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(1)

    readFileToString("streamReactorBackups/mytopic/1/2.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}{"name":"tom","title":null,"salary":395.44}""")

  }

  "S3SinkTask" should "flush for every record when configured flush count size of 1" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(3)

    readFileToString("streamReactorBackups/mytopic/1/0.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}""")
    readFileToString("streamReactorBackups/mytopic/1/1.json", blobStoreContext) should be("""{"name":"laura","title":"ms","salary":429.06}""")
    readFileToString("streamReactorBackups/mytopic/1/2.json", blobStoreContext) should be("""{"name":"tom","title":null,"salary":395.44}""")

  }

  /**
    * The file sizes of the 3 records above come out as 44,46,44.
    * We're going to set the threshold to 80 - so once we've written 2 records to a file then the
    * second file should only contain a single record.  This second file won't have been written yet.
    */
  "S3SinkTask" should "flush on configured file size" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_SIZE = 80"
        )
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(1)

    readFileToString("streamReactorBackups/mytopic/1/1.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}""")

  }

  /**
    * The difference in this test is that the sink is opened again, which will cause the offsets to be copied to the
    * context
    */
  "S3SinkTask" should "put existing offsets to the context" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1"
        )
      ).asJava

    val sinkTaskContext = mock[SinkTaskContext]
    task.initialize(sinkTaskContext)
    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(3)

    readFileToString("streamReactorBackups/mytopic/1/0.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}""")
    readFileToString("streamReactorBackups/mytopic/1/1.json", blobStoreContext) should be("""{"name":"laura","title":"ms","salary":429.06}""")
    readFileToString("streamReactorBackups/mytopic/1/2.json", blobStoreContext) should be("""{"name":"tom","title":null,"salary":395.44}""")

    verify(sinkTaskContext).offset(new TopicPartition("mytopic", 1), 2)
  }

  "S3SinkTask" should "write to parquet format" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"""insert into $BucketName:$PrefixName select * from $TopicName STOREAS `PARQUET` WITH_FLUSH_COUNT = 1""")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(3)

    val bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/0.parquet", blobStoreContext)

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords(0), "sam", "mr", 100.43)

  }

  "S3SinkTask" should "write to avro format" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(3)

    val bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/0.avro", blobStoreContext)

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords(0), "sam", "mr", 100.43)

  }


  "S3SinkTask" should "error when trying to write AVRO to text format" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `TEXT` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    assertThrows[IllegalStateException] {
      task.put(records.asJava)
    }

    task.stop()
  }

  "S3SinkTask" should "write to text format" in {

    val textRecords = List(
      new SinkRecord(TopicName, 1, null, null, null, "Sausages", 0),
      new SinkRecord(TopicName, 1, null, null, null, "Mash", 1),
      new SinkRecord(TopicName, 1, null, null, null, "Peas", 2),
      new SinkRecord(TopicName, 1, null, null, null, "Gravy", 3)
    )
    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `TEXT` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(2)

    val file1Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/1.text", blobStoreContext)
    new String(file1Bytes) should be("Sausages\nMash\n")

    val file2Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/3.text", blobStoreContext)
    new String(file2Bytes) should be("Peas\nGravy\n")

  }

  "S3SinkTask" should "write to csv format with headers" in {

    val task = new S3SinkTask()

    val extraRecord = new SinkRecord(TopicName, 1, null, null, schema,
      new Struct(schema).put("name", "bob").put("title", "mr").put("salary", 200.86), 3)

    val allRecords: List[SinkRecord] = records :+ extraRecord

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `CSV_WITHHEADERS` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(allRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(2)

    val file1Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/1.csv", blobStoreContext)

    val file1Reader = new StringReader(new String(file1Bytes))
    val file1CsvReader = new CSVReader(file1Reader)

    file1CsvReader.readNext() should be(Array("name", "title", "salary"))
    file1CsvReader.readNext() should be(Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be(Array("laura", "ms", "429.06"))
    file1CsvReader.readNext() should be(null)

    val file2Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/3.csv", blobStoreContext)

    val file2Reader = new StringReader(new String(file2Bytes))
    val file2CsvReader = new CSVReader(file2Reader)

    file2CsvReader.readNext() should be(Array("name", "title", "salary"))
    file2CsvReader.readNext() should be(Array("tom", "", "395.44"))
    file2CsvReader.readNext() should be(Array("bob", "mr", "200.86"))
    file2CsvReader.readNext() should be(null)
  }

  "S3SinkTask" should "write to csv format without headers" in {

    val task = new S3SinkTask()

    val extraRecord = new SinkRecord(TopicName, 1, null, null, schema,
      new Struct(schema).put("name", "bob").put("title", "mr").put("salary", 200.86), 3)

    val allRecords: List[SinkRecord] = records :+ extraRecord

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `CSV` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(allRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(2)

    val file1Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/1.csv", blobStoreContext)

    val file1Reader = new StringReader(new String(file1Bytes))
    val file1CsvReader = new CSVReader(file1Reader)

    file1CsvReader.readNext() should be(Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be(Array("laura", "ms", "429.06"))
    file1CsvReader.readNext() should be(null)

    val file2Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/3.csv", blobStoreContext)

    val file2Reader = new StringReader(new String(file2Bytes))
    val file2CsvReader = new CSVReader(file2Reader)

    file2CsvReader.readNext() should be(Array("tom", "", "395.44"))
    file2CsvReader.readNext() should be(Array("bob", "mr", "200.86"))
    file2CsvReader.readNext() should be(null)
  }

  "S3SinkTask" should "use custom partitioning scheme and flush for every record" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name,title,salary WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(headerPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.recursive().prefix("streamReactorBackups/")).size() should be(6)

    readFileToString("streamReactorBackups/name=first/title=primary/salary=[missing]/mytopic(1_0).json", blobStoreContext) should be("""{"name":"first","title":"primary","salary":null}""")
    readFileToString("streamReactorBackups/name=second/title=secondary/salary=100.0/mytopic(1_1).json", blobStoreContext) should be("""{"name":"second","title":"secondary","salary":100.0}""")
    readFileToString("streamReactorBackups/name=third/title=primary/salary=100.0/mytopic(1_2).json", blobStoreContext) should be("""{"name":"third","title":"primary","salary":100.0}""")
    readFileToString("streamReactorBackups/name=first/title=[missing]/salary=200.0/mytopic(1_3).json", blobStoreContext) should be("""{"name":"first","title":null,"salary":200.0}""")
    readFileToString("streamReactorBackups/name=second/title=[missing]/salary=100.0/mytopic(1_4).json", blobStoreContext) should be("""{"name":"second","title":null,"salary":100.0}""")
    readFileToString("streamReactorBackups/name=third/title=[missing]/salary=100.0/mytopic(1_5).json", blobStoreContext) should be("""{"name":"third","title":null,"salary":100.0}""")

  }

  /**
    * As soon as one file is eligible for writing, it will write all those from the same topic partition.  Therefore 4
    * files are written instead of 2, as there are 2 points at which the write is triggered and the half-full files must
    * be written as well as those reaching the threshold.
    */
  "S3SinkTask" should "use custom partitioning scheme and flush after two written records" in {

    val task = new S3SinkTask()

    val partitionedData: List[Struct] = List(
      new Struct(schema).put("name", "first").put("title", "primary").put("salary", null),
      new Struct(schema).put("name", "first").put("title", "secondary").put("salary", 100.00),
      new Struct(schema).put("name", "first").put("title", "primary").put("salary", 100.00),
      new Struct(schema).put("name", "second").put("title", "secondary").put("salary", 200.00),
      new Struct(schema).put("name", "second").put("title", "primary").put("salary", 100.00),
      new Struct(schema).put("name", "second").put("title", "secondary").put("salary", 100.00)
    )

    val records = partitionedData.zipWithIndex.map { case (user, k) =>
      new SinkRecord(TopicName, 1, null, null, schema, user, k)
    }

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name,title WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.recursive().prefix("streamReactorBackups/")).size() should be(4)

    readFileToString("streamReactorBackups/name=first/title=primary/mytopic(1_2).json", blobStoreContext) should be("""{"name":"first","title":"primary","salary":null}{"name":"first","title":"primary","salary":100.0}""")
    readFileToString("streamReactorBackups/name=first/title=secondary/mytopic(1_1).json", blobStoreContext) should be("""{"name":"first","title":"secondary","salary":100.0}""")
    readFileToString("streamReactorBackups/name=second/title=secondary/mytopic(1_5).json", blobStoreContext) should be("""{"name":"second","title":"secondary","salary":200.0}{"name":"second","title":"secondary","salary":100.0}""")
    readFileToString("streamReactorBackups/name=second/title=primary/mytopic(1_4).json", blobStoreContext) should be("""{"name":"second","title":"primary","salary":100.0}""")

  }

  "S3SinkTask" should "use custom partitioning with value display only" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name,title,salary WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(headerPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.recursive().prefix("streamReactorBackups/")).size() should be(6)

    readFileToString("streamReactorBackups/first/primary/[missing]/mytopic(1_0).json", blobStoreContext) should be("""{"name":"first","title":"primary","salary":null}""")
    readFileToString("streamReactorBackups/second/secondary/100.0/mytopic(1_1).json", blobStoreContext) should be("""{"name":"second","title":"secondary","salary":100.0}""")
    readFileToString("streamReactorBackups/third/primary/100.0/mytopic(1_2).json", blobStoreContext) should be("""{"name":"third","title":"primary","salary":100.0}""")
    readFileToString("streamReactorBackups/first/[missing]/200.0/mytopic(1_3).json", blobStoreContext) should be("""{"name":"first","title":null,"salary":200.0}""")
    readFileToString("streamReactorBackups/second/[missing]/100.0/mytopic(1_4).json", blobStoreContext) should be("""{"name":"second","title":null,"salary":100.0}""")
    readFileToString("streamReactorBackups/third/[missing]/100.0/mytopic(1_5).json", blobStoreContext) should be("""{"name":"third","title":null,"salary":100.0}""")

  }

  "S3SinkTask" should "write to bytes format and combine files" in {

    val stream = classOf[BytesFormatWriter].getResourceAsStream("/streamreactor-logo.png")
    val bytes: Array[Byte] = IOUtils.toByteArray(stream)
    val (bytes1, bytes2) = bytes.splitAt(bytes.length / 2)

    val textRecords = List(
      new SinkRecord(TopicName, 1, null, null, null, bytes1, 0),
      new SinkRecord(TopicName, 1, null, null, null, bytes2, 1)
    )
    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `BYTES_ValueOnly` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(1)

    val file1Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/1.bytes", blobStoreContext)
    file1Bytes should be(bytes)

  }


  "S3SinkTask" should "support header values for data partitioning" in {

    val textRecords = List(
      new SinkRecord(TopicName, 1, null, null, null, users(0), 0, null, null, createHeaders(("phoneprefix", "+44"), ("region", "8"))),
      new SinkRecord(TopicName, 1, null, null, null, users(1), 1, null, null, createHeaders(("phoneprefix", "+49"), ("region", "5"))),
      new SinkRecord(TopicName, 1, null, null, null, users(2), 2, null, null, createHeaders(("phoneprefix", "+49"), ("region", "5")))
    )
    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.phoneprefix,_header.region STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive()).size() should be(3)

    val file1CsvReader: CSVReader = openCsvReaderToBucketFile("streamReactorBackups/phoneprefix=+44/region=8/mytopic(1_0).csv")
    file1CsvReader.readNext() should be(Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be(null)

    val file2CsvReader: CSVReader = openCsvReaderToBucketFile("streamReactorBackups/phoneprefix=+49/region=5/mytopic(1_1).csv")
    file2CsvReader.readNext() should be(Array("laura", "ms", "429.06"))
    file2CsvReader.readNext() should be(null)

    val file3CsvReader: CSVReader = openCsvReaderToBucketFile("streamReactorBackups/phoneprefix=+49/region=5/mytopic(1_2).csv")
    file3CsvReader.readNext() should be(Array("tom", "", "395.44"))
    file3CsvReader.readNext() should be(null)

  }

  "S3SinkTask" should "combine header and value-extracted partition" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.headerPartitionKey,_value.name STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(headerPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(6)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/headerPartitionKey=0/name=first/mytopic(1_0).csv",
      "streamReactorBackups/headerPartitionKey=1/name=second/mytopic(1_1).csv",
      "streamReactorBackups/headerPartitionKey=0/name=third/mytopic(1_2).csv",
      "streamReactorBackups/headerPartitionKey=1/name=first/mytopic(1_3).csv",
      "streamReactorBackups/headerPartitionKey=0/name=second/mytopic(1_4).csv",
      "streamReactorBackups/headerPartitionKey=1/name=third/mytopic(1_5).csv"
    )
  }

  "S3SinkTask" should "fail when header value is missing from the record when header partitioning activated" in {

    val textRecords = List(
      createSinkRecord(1, users(0), 0, createHeaders(("feet", "5"))),
      createSinkRecord(1, users(1), 0, createHeaders(("hair", "blue"), ("feet", "5"))),
    )

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.hair,_header.feet STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val caught = intercept[IllegalArgumentException] {
      task.put(textRecords.asJava)
    }

    assert(caught.getMessage.equalsIgnoreCase("Header 'hair' not found in message"))

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive()).size() should be(0)

  }

  "S3SinkTask" should "support numeric header data types" in {

    val textRecords = List(
      createSinkRecord(1, users(0), 0, createHeaders(("intheader", 1), ("longheader", 2L))),
      createSinkRecord(1, users(1), 1, createHeaders(("longheader", 2L), ("intheader", 2))),
      createSinkRecord(2, users(2), 2, createHeaders(("intheader", 1), ("longheader", 1L))),
    )
    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.intheader,_header.longheader STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(3)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/intheader=1/longheader=2/mytopic(1_0).csv",
      "streamReactorBackups/intheader=2/longheader=2/mytopic(1_1).csv",
      "streamReactorBackups/intheader=1/longheader=1/mytopic(2_2).csv",
    )
  }

  "S3SinkTask" should "partition by whole of kafka message key with key label" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map { case (user, k) =>
      new SinkRecord(TopicName, 1, null, (k % 2).toString, schema, user, k, null, null)
    }

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(6)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/key=0/mytopic(1_0).csv",
      "streamReactorBackups/key=1/mytopic(1_1).csv",
      "streamReactorBackups/key=0/mytopic(1_2).csv",
      "streamReactorBackups/key=1/mytopic(1_3).csv",
      "streamReactorBackups/key=0/mytopic(1_4).csv",
      "streamReactorBackups/key=1/mytopic(1_5).csv"
    )
  }

  "S3SinkTask" should "partition by whole of kafka message key without key label" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map { case (user, k) =>
      new SinkRecord(TopicName, 1, null, (k % 2).toString, schema, user, k, null, null)
    }

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(6)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/0/mytopic(1_0).csv",
      "streamReactorBackups/1/mytopic(1_1).csv",
      "streamReactorBackups/0/mytopic(1_2).csv",
      "streamReactorBackups/1/mytopic(1_3).csv",
      "streamReactorBackups/0/mytopic(1_4).csv",
      "streamReactorBackups/1/mytopic(1_5).csv"
    )
  }

  "S3SinkTask" should "fail if the key is non-primitive but requests _key partitioning" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map { case (user, k) =>
      new SinkRecord(TopicName, 1, null, user, schema, user, k, null, null)
    }

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val intercepted = intercept[IllegalStateException] {
      task.put(keyPartitionedRecords.asJava)
    }
    intercepted.getMessage should be("Non primitive struct provided, PARTITIONBY _key requested in KCQL")

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(0)

  }

  "S3SinkTask" should "allow a primitive int key for _key partitioning" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map { case (user, k) =>
      new SinkRecord(TopicName, 1, null, k % 2, schema, user, k, null, null)
    }

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(6)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/0/mytopic(1_0).csv",
      "streamReactorBackups/1/mytopic(1_1).csv",
      "streamReactorBackups/0/mytopic(1_2).csv",
      "streamReactorBackups/1/mytopic(1_3).csv",
      "streamReactorBackups/0/mytopic(1_4).csv",
      "streamReactorBackups/1/mytopic(1_5).csv"
    )
  }

  "S3SinkTask" should "allow partitioning by complex key" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key.region, _key.phoneprefix STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(3)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/region=8/phoneprefix=+44/mytopic(1_0).csv",
      "streamReactorBackups/region=5/phoneprefix=+49/mytopic(1_1).csv",
      "streamReactorBackups/region=5/phoneprefix=+49/mytopic(1_2).csv"
    )
  }

  "S3SinkTask" should "allow partitioning by complex key and values" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key.region, name WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(3)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/region=8/name=sam/mytopic(1_0).json",
      "streamReactorBackups/region=5/name=laura/mytopic(1_1).json",
      "streamReactorBackups/region=5/name=tom/mytopic(1_2).json"
    )
  }

  /**
    * This should write partition 1 but not partition 0
    */
  "S3SinkTask" should "write multiple partitions independently" in {

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, schema, users(0), 0),
      new SinkRecord(TopicName, 1, null, null, schema, users(1), 0),
      new SinkRecord(TopicName, 1, null, null, schema, users(2), 1)
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
      new TopicPartition(TopicName, 1)
    ).asJava

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(1)

    fileList.asScala.map(file => file.getName) should contain(
      "streamReactorBackups/mytopic/1/1.json",
    )
  }

  private def createSinkRecord(partition: Int, valueStruct: Struct, offset: Int, headers: lang.Iterable[Header]) = {
    new SinkRecord(TopicName, partition, null, null, null, valueStruct, offset, null, null, headers)
  }

  private def createKey(keySchema: Schema, keyValuePair: (String, Any)*): Struct = {
    val struct = new Struct(keySchema)
    keyValuePair.foreach {
      case (key: String, value) => struct.put(key, value)
    }
    struct
  }

  private def createHeaders[T](keyValuePair: (String, T)*): lang.Iterable[Header] = {
    val headers = new ConnectHeaders()
    keyValuePair.foreach {
      case (key: String, value) => headers.add(key, value, null)
    }
    headers
  }

  private def openCsvReaderToBucketFile(bucketFileName: String) = {
    val file1Bytes = S3TestPayloadReader.readPayload(BucketName, bucketFileName, blobStoreContext)

    val file1Reader = new StringReader(new String(file1Bytes))
    new CSVReader(file1Reader)
  }
}