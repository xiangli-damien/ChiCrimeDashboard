package com.xiangli;

/**
 * Chicago Crime Kafka Producer
 * Fetches crime data (JSON format) from Chicago Data API and sends it to a Kafka topic.
 *
 * @author lixiang
 * @version 1.0
 * @created 2024/12/08
 */

import org.apache.kafka.clients.producer.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.Properties;

public class ChicagoCrimeKafkaProducer {

    public static void main(String[] args) {
        // Kafka broker configuration
        String bootstrapServers = "wn0-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092,wn1-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092";

        // API URL for fetching JSON data
        String jsonUrl = "https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit=3&$order=date%20DESC&$$app_token=qTBRg4J34JXWUSAwzN6lUGRAn";

        // Kafka topic for publishing JSON data
        String jsonTopic = "xli0808_latest-crime-json";

        try {
            // Fetch data from the JSON API
            String jsonData = fetchData(jsonUrl);

            // Publish the fetched JSON data to the Kafka topic
            sendToKafka(bootstrapServers, jsonTopic, jsonData);
            System.out.println("Sent JSON crime data to Kafka topic: " + jsonTopic);

        } catch (Exception e) {
            // Handle any exceptions that occur during data fetching or Kafka publishing
            e.printStackTrace();
        }
    }

    /**
     * Fetches data from the specified URL using Apache HttpClient.
     *
     * @param url API URL to fetch data from
     * @return String containing the response data from the API
     * @throws Exception if there is an error during the HTTP request
     */
    public static String fetchData(String url) throws Exception {
        // Create a CloseableHttpClient instance for making HTTP requests
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet request = new HttpGet(url); // Create an HTTP GET request
        CloseableHttpResponse response = httpClient.execute(request); // Execute the request

        try {
            // Convert the response entity to a string and return it
            return EntityUtils.toString(response.getEntity());
        } finally {
            // Ensure that resources are properly closed to prevent memory leaks
            response.close();
            httpClient.close();
        }
    }

    /**
     * Sends data to a specified Kafka topic.
     *
     * @param bootstrapServers Kafka bootstrap server addresses
     * @param topicName Kafka topic to publish data to
     * @param data Data to be sent to the Kafka topic
     */
    public static void sendToKafka(String bootstrapServers, String topicName, String data) {
        // Configure Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers); // Kafka cluster bootstrap servers
        props.put("acks", "all"); // Ensure all replicas acknowledge the message
        props.put("retries", 0); // No retries for failed sends
        props.put("linger.ms", 1); // Minimal batching delay
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Key serializer
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Value serializer

        // Create a KafkaProducer instance
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Create a producer record with the specified topic and data
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, data);

            // Send the record asynchronously and handle the callback for success or failure
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        // Log any exceptions encountered during sending
                        e.printStackTrace();
                    } else {
                        // Log successful metadata, including partition and offset
                        System.out.println("Sent record to partition " + metadata.partition() + " with offset " + metadata.offset());
                    }
                }
            });
        } finally {
            // Ensure the producer is flushed and closed to release resources
            producer.flush();
            producer.close();
        }
    }
}
