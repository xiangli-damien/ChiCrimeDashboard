package com.xiangli

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConverters._

object ChicagoCrimes {

  // Kafka configuration
  val bootstrapServers = "wn0-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092,wn1-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092"
  val topic = "xli0808_latest-crime-json"

  // Jackson ObjectMapper for parsing JSON
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // HBase configuration and table connection
  val hbaseConf: Configuration = HBaseConfiguration.create()
  val hbaseConnection: Connection = ConnectionFactory.createConnection(hbaseConf)
  val crimeTable: Table = hbaseConnection.getTable(TableName.valueOf("xli0808_speed_latest_crimes_table"))

  /**
   * Write a single crime record to HBase
   *
   * @param table HBase Table instance
   * @param crime JsonNode representing the crime record
   */
  def writeCrimeToHBase(table: Table, crime: JsonNode): Unit = {
    val date = crime.path("date").asText("")
    val community_area = crime.path("community_area").asText("")
    val primary_type = crime.path("primary_type").asText("UNKNOWN")
    val latitude = crime.path("latitude").asText("0.0")
    val longitude = crime.path("longitude").asText("0.0")

    // Skip record if required fields are missing
    if (date.isEmpty || community_area.isEmpty) {
      println(s"Skipping record due to missing fields: date=$date, community_area=$community_area")
      return
    }

    // Format the date as yyyyMMdd and generate a unique rowKey
    val formattedDate = formatDate(date)
    val timestamp = System.currentTimeMillis().toString
    val rowKey = s"${formattedDate}_${community_area}_$timestamp"

    // Prepare the Put object
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date"), Bytes.toBytes(formattedDate))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("community_area"), Bytes.toBytes(community_area))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("primary_type"), Bytes.toBytes(primary_type))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("latitude"), Bytes.toBytes(latitude))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("longitude"), Bytes.toBytes(longitude))

    // Write to HBase
    table.put(put)
    println(s"Inserted into HBase: rowKey=$rowKey, date=$formattedDate, community_area=$community_area, type=$primary_type, latitude=$latitude, longitude=$longitude")
  }

  /**
   * Convert a date in yyyy-MM-dd format to yyyyMMdd format
   *
   * @param dateStr Original date string
   * @return Formatted date string
   */
  def formatDate(dateStr: String): String = {
    if (dateStr.length >= 10) {
      val yyyy = dateStr.substring(0, 4)
      val mm = dateStr.substring(5, 7)
      val dd = dateStr.substring(8, 10)
      s"$yyyy$mm$dd"
    } else {
      "unknown_date"
    }
  }

  def main(args: Array[String]): Unit = {

    // Spark Streaming configuration
    val sparkConf = new SparkConf().setAppName("ChicagoCrimesStream")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "chicago_crime_stream_group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)

    // Create Kafka Direct Stream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // Extract JSON strings
    val jsonRecords = stream.map(_.value())

    // Process each RDD in the stream
    jsonRecords.foreachRDD { rdd =>
      if (rdd.isEmpty()) {
        println("No data received this batch.")
      } else {
        println("Received a batch of messages.")
        rdd.foreach { recordValue =>
          try {
            val rootNode = mapper.readTree(recordValue)
            if (rootNode.isArray) {
              // Iterate over each crime JSON in the array
              rootNode.elements().asScala.foreach { crimeJson =>
                writeCrimeToHBase(crimeTable, crimeJson)
              }
            } else {
              println(s"Skipping non-array message: $recordValue")
            }
          } catch {
            case e: Exception =>
              println(s"Failed to parse JSON: $recordValue, error: ${e.getMessage}")
          }
        }
      }
    }

    // Start StreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}
