package com.spredfast.kafka.connect.s3;

import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.S3Object;
import io.debezium.testing.testcontainers.Connector.State;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.mguenther.kafka.junit.ExternalKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import net.mguenther.kafka.junit.SendValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class S3SinkConnectorIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3SinkConnectorIT.class);

  private static final Network network = Network.newNetwork();

  private AmazonS3 s3;

  private static final KafkaContainer kafkaContainer =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.2")).withNetwork(network);

  public static LocalStackContainer localStackContainer =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.13.3"))
          .withNetworkAliases("localstack")
          .withNetwork(network)
          .withServices(Service.S3);

  public static DebeziumContainer kafkaConnectContainer =
      DebeziumContainer.latestStable()
          .withFileSystemBind("build/libs", "/kafka/connect/s3-sink-connector")
          .withCopyToContainer(
              MountableFile.forClasspathResource("log4j.properties"),
              "/kafka/config/log4j.properties")
          .withNetwork(network)
          .withKafka(kafkaContainer)
          .withLogConsumer(new Slf4jLogConsumer(LOGGER))
          .withEnv("AWS_ACCESS_KEY_ID", localStackContainer.getAccessKey())
          .withEnv("AWS_SECRET_ACCESS_KEY", localStackContainer.getSecretKey())
          .withEnv("AWS_DEFAULT_REGION", localStackContainer.getRegion())
          .withEnv("OFFSET_FLUSH_INTERVAL_MS", "20000")
          .withEnv("CONNECT_CONSUMER_METADATA_MAX_AGE_MS", "1000")
          .dependsOn(kafkaContainer);

  private ExternalKafkaCluster kafkaCluster =
      ExternalKafkaCluster.at(kafkaContainer.getBootstrapServers());

  private Admin kafkaAdmin;
  private KafkaProducer<String, String> kafkaProducer;

  @BeforeClass
  public static void startContainers() {
    Startables.deepStart(Stream.of(kafkaContainer, localStackContainer, kafkaConnectContainer))
        .join();
  }

  @Before
  public void setup() {
    s3 =
        AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(
                    localStackContainer.getEndpointOverride(Service.S3).toString(),
                    localStackContainer.getRegion()))
            .withCredentials(
                new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(
                        localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
            .build();

    kafkaCluster = ExternalKafkaCluster.at(kafkaContainer.getBootstrapServers());
    kafkaAdmin =
        Admin.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()));
    kafkaProducer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaContainer.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class,
                ProducerConfig.ACKS_CONFIG,
                "all",
                ProducerConfig.RETRIES_CONFIG,
                "1"));
  }

  @AfterClass
  public static void tearDown() {
    localStackContainer.close();
    kafkaConnectContainer.close();
    kafkaContainer.close();
  }

  @Test
  public void testSinkWithGzipFileThreshold() throws Exception {
    String bucketName = "connect-system-test-size";
    String prefix = "systest";
    String topicName = "system_test_size";
    String connectorName = "s3-sink-size";
    s3.createBucket(bucketName);

    // Create the test topic
    kafkaCluster.createTopic(TopicConfig.withName(topicName).withNumberOfPartitions(1));

    // Define, register, and start the connector
    ConnectorConfiguration connectorConfiguration =
        getConnectorConfiguration(
            connectorName, bucketName, prefix, topicName, 3000, 250, 12 * 3600 * 1000);
    kafkaConnectContainer.registerConnector(connectorName, connectorConfiguration);
    kafkaConnectContainer.ensureConnectorTaskState(connectorName, 0, State.RUNNING);

    assertEquals(-1, getConsumerOffset("connect-" + connectorName, topicName, 0));

    // Produce messages to the topic (uncompressed: 3190, compressed: ~340)
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 95; i++) {
      String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
      kafkaCluster.send(SendValues.to(topicName, json));
      sb.append(json).append("\n");
    }
    for (int i = 95; i < 100; i++) {
      String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
      kafkaCluster.send(SendValues.to(topicName, json));
    }
    String expectedDataObjectContents = sb.toString();
    LocalDate today = LocalDate.now(ZoneOffset.UTC);

    // Wait for the last chunk object
    final String lastChunkIndexObjectKeyPartition0 =
        String.format("%s/last_chunk_index.%s-00000.txt", prefix, topicName);
    Awaitility.await()
        .pollDelay(1, TimeUnit.SECONDS)
        .atMost(60, TimeUnit.SECONDS)
        .until(() -> objectKeyExists(bucketName, lastChunkIndexObjectKeyPartition0));

    // Validate partition 0's last chunk object
    String indexObjectKey =
        String.format("%s/%s/%s-00000-000000000000.index.json", prefix, today, topicName);
    String objectContents = getS3FileOutput(bucketName, lastChunkIndexObjectKeyPartition0);
    assertEquals(indexObjectKey + "\n", objectContents);

    // Validate partition 0's data object
    String dataObjectKey =
        String.format("%s/%s/%s-00000-000000000000.gz", prefix, today, topicName);
    objectContents = getS3FileOutput(bucketName, dataObjectKey);
    assertEquals(expectedDataObjectContents, objectContents);

    // Validate partition 0's index object
    String expectedIndexObjectContents =
        "{\"chunks\":["
            + "{\"byte_length_uncompressed\":2998,\"num_records\":94,\"byte_length\":270,\"byte_offset\":0,\"first_record_offset\":0},"
            + "{\"byte_length_uncompressed\":32,\"num_records\":1,\"byte_length\":51,\"byte_offset\":270,\"first_record_offset\":94}]}\n";
    objectContents = getS3FileOutput(bucketName, indexObjectKey);
    assertEquals(expectedIndexObjectContents, objectContents);

    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .until(() -> getConsumerOffset("connect-" + connectorName, topicName, 0) == 95);

    // It's not enough to trigger another flush
    for (int i = 100; i < 150; i++) {
      kafkaCluster.send(
          SendValues.to(topicName, String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i)));
    }

    // The new index object won't be written as gzip file is not big enough
    String indexObjectKey2 =
        String.format("%s/%s/%s-00000-000000000095.index.json", prefix, today, topicName);
    Awaitility.await()
        .pollDelay(5, TimeUnit.SECONDS)
        .during(20, TimeUnit.SECONDS)
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> !objectKeyExists(bucketName, indexObjectKey2));

    // Make sure the committed offset didn't change as there were no new file uploads
    assertEquals(95, getConsumerOffset("connect-" + connectorName, topicName, 0));

    // Delete the connector
    deleteConnector(connectorName);
  }

  @Test
  public void testSinkWithBigFlushInterval() throws Exception {
    String bucketName = "connect-system-test-12h";
    String prefix = "systest";
    String topicName = "system_test_12h";
    String connectorName = "s3-sink-12h";
    s3.createBucket(bucketName);

    // Create the test topic
    kafkaCluster.createTopic(TopicConfig.withName(topicName).withNumberOfPartitions(1));

    // Define, register, and start the connector
    final long flushIntevalMs = 12 * 3600 * 1000;
    ConnectorConfiguration connectorConfiguration =
        getConnectorConfiguration(
            connectorName,
            bucketName,
            prefix,
            topicName,
            67108864,
            100 * 1024 * 1024,
            flushIntevalMs);
    kafkaConnectContainer.registerConnector(connectorName, connectorConfiguration);
    kafkaConnectContainer.ensureConnectorTaskState(connectorName, 0, State.RUNNING);

    // Produce messages to the topic
    StringBuilder sb = new StringBuilder();
    long ts = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
      kafkaProducer.send(new ProducerRecord<>(topicName, 0, ts, "", json));
      sb.append(json).append("\n");
    }
    LocalDate today = LocalDate.now(ZoneOffset.UTC);

    // Define the expected files
    String dataObjectKey =
        String.format("%s/%s/%s-00000-000000000000.gz", prefix, today, topicName);
    String indexObjectKey =
        String.format("%s/%s/%s-00000-000000000000.index.json", prefix, today, topicName);
    String lastChunkIndexObjectKey =
        String.format("%s/last_chunk_index.%s-00000.txt", prefix, topicName);
    List<String> expectedObjects = List.of(indexObjectKey, dataObjectKey, lastChunkIndexObjectKey);

    // Make sure no files are produced yet as the timestamp based flushing threshold is not reached
    Awaitility.await()
        .pollDelay(5, TimeUnit.SECONDS)
        .during(20, TimeUnit.SECONDS)
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> expectedObjects.stream().noneMatch(key -> objectKeyExists(bucketName, key)));

    assertEquals(-1, getConsumerOffset("connect-" + connectorName, topicName, 0));

    ts += flushIntevalMs + 60 * 1000;
    for (int i = 100; i < 200; i++) {
      String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
      kafkaProducer.send(new ProducerRecord<>(topicName, 0, ts, "", json));
    }

    // Await for all the files to be produced by all tasks
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> expectedObjects.stream().allMatch(key -> objectKeyExists(bucketName, key)));

    // Check the data object contains expected records
    // in particular, it shouldn't contain the record that triggered flushing
    String objectContents = getS3FileOutput(bucketName, dataObjectKey);
    assertEquals(sb.toString(), objectContents);

    // Check that the committed offset corresponds to the message offset that triggered timestamp
    // based flushing
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> getConsumerOffset("connect-" + connectorName, topicName, 0) == 100);

    // Delete the connector
    deleteConnector(connectorName);
  }

  @Test
  public void testSinkWithRestart() throws Exception {
    String bucketName = "connect-system-test";
    String prefix = "systest";
    String topicName = "system_test";
    String connectorName = "s3-sink";
    s3.createBucket(bucketName);

    // Create the test topic
    kafkaCluster.createTopic(TopicConfig.withName(topicName).withNumberOfPartitions(1));

    // Define, register, and start the connector
    ConnectorConfiguration connectorConfiguration =
        getConnectorConfiguration(connectorName, bucketName, prefix, topicName);
    kafkaConnectContainer.registerConnector(connectorName, connectorConfiguration);
    kafkaConnectContainer.ensureConnectorTaskState(connectorName, 0, State.RUNNING);

    // Produce messages to the topic
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
      kafkaCluster.send(SendValues.to(topicName, json));
      sb.append(json).append("\n");
    }
    String expectedDataObjectContents = sb.toString();
    LocalDate today = LocalDate.now(ZoneOffset.UTC);

    // Wait for the last chunk object
    final String lastChunkIndexObjectKeyPartition0 =
        String.format("%s/last_chunk_index.%s-00000.txt", prefix, topicName);
    Awaitility.await()
        .pollDelay(1, TimeUnit.SECONDS)
        .atMost(60, TimeUnit.SECONDS)
        .until(() -> objectKeyExists(bucketName, lastChunkIndexObjectKeyPartition0));

    // Validate partition 0's last chunk object
    String indexObjectKey =
        String.format("%s/%s/%s-00000-000000000000.index.json", prefix, today, topicName);
    String objectContents = getS3FileOutput(bucketName, lastChunkIndexObjectKeyPartition0);
    assertEquals(indexObjectKey + "\n", objectContents);

    // Validate partition 0's data object
    String dataObjectKey =
        String.format("%s/%s/%s-00000-000000000000.gz", prefix, today, topicName);
    objectContents = getS3FileOutput(bucketName, dataObjectKey);
    assertEquals(expectedDataObjectContents, objectContents);

    // Validate partition 0's index object
    String expectedIndexObjectContents =
        "{\"chunks\":[{\"byte_length_uncompressed\":3190,\"num_records\":100,\"byte_length\":283,\"byte_offset\":0,\"first_record_offset\":0}]}\n";
    objectContents = getS3FileOutput(bucketName, indexObjectKey);
    assertEquals(expectedIndexObjectContents, objectContents);

    // Delete the connector
    deleteConnector(connectorName);

    // Produce messages while the connector is not running
    sb.setLength(0);
    for (int i = 100; i < 200; i++) {
      String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
      kafkaCluster.send(SendValues.to(topicName, json));
      sb.append(json).append("\n");
    }
    expectedDataObjectContents = sb.toString();

    // Redeploy the connector
    kafkaConnectContainer.registerConnector(connectorName, connectorConfiguration);
    kafkaConnectContainer.ensureConnectorTaskState(connectorName, 0, State.RUNNING);

    // Await for the new index object to be written
    String indexObjectKey2 =
        String.format("%s/%s/%s-00000-000000000100.index.json", prefix, today, topicName);
    Awaitility.await()
        .pollDelay(1, TimeUnit.SECONDS)
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> objectKeyExists(bucketName, indexObjectKey2));

    // Validate partition 0's last chunk object was updated
    objectContents = getS3FileOutput(bucketName, lastChunkIndexObjectKeyPartition0);
    assertEquals(indexObjectKey2 + "\n", objectContents);

    // Validate partition 0's data object
    dataObjectKey = String.format("%s/%s/%s-00000-000000000100.gz", prefix, today, topicName);
    objectContents = getS3FileOutput(bucketName, dataObjectKey);
    assertEquals(expectedDataObjectContents, objectContents);

    // Validate partition 0's index object
    expectedIndexObjectContents =
        "{\"chunks\":[{\"byte_length_uncompressed\":3300,\"num_records\":100,\"byte_length\":278,\"byte_offset\":0,\"first_record_offset\":100}]}\n";
    objectContents = getS3FileOutput(bucketName, indexObjectKey2);
    assertEquals(expectedIndexObjectContents, objectContents);

    // Increase the partitions from 1 to 3
    kafkaContainer.execInContainer(
        "kafka-topics",
        "--bootstrap-server",
        "localhost:9092",
        "--alter",
        "--topic",
        topicName,
        "--partitions",
        "3");

    // Restart the connector to pick up the partition changes
    deleteConnector(connectorName);
    kafkaConnectContainer.registerConnector(connectorName, connectorConfiguration);
    kafkaConnectContainer.ensureConnectorTaskState(connectorName, 0, State.RUNNING);

    for (int i = 200; i < 300; i++) {
      String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
      kafkaCluster.send(
          SendKeyValues.to(
              topicName, Collections.singletonList(new KeyValue<>(String.valueOf(i), json))));
    }

    // Define the expected files
    List<String> expectedObjects =
        List.of(
            String.format("%s/%s/%s-00000-000000000200.gz", prefix, today, topicName),
            String.format("%s/%s/%s-00000-000000000200.index.json", prefix, today, topicName),
            String.format("%s/%s/%s-00001-000000000000.gz", prefix, today, topicName),
            String.format("%s/%s/%s-00001-000000000000.index.json", prefix, today, topicName),
            String.format("%s/%s/%s-00002-000000000000.gz", prefix, today, topicName),
            String.format("%s/%s/%s-00002-000000000000.index.json", prefix, today, topicName),
            String.format("%s/last_chunk_index.%s-00000.txt", prefix, topicName),
            String.format("%s/last_chunk_index.%s-00001.txt", prefix, topicName),
            String.format("%s/last_chunk_index.%s-00002.txt", prefix, topicName));

    // Await for all the files to be produced by all tasks
    Awaitility.await()
        .pollDelay(1, TimeUnit.SECONDS)
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> expectedObjects.stream().allMatch(key -> objectKeyExists(bucketName, key)));

    // Validate partition 0's index object
    indexObjectKey =
        String.format("%s/%s/%s-00000-000000000200.index.json", prefix, today, topicName);
    objectContents = getS3FileOutput(bucketName, lastChunkIndexObjectKeyPartition0);
    assertEquals(indexObjectKey + "\n", objectContents);

    // Validate partition 1's index object
    String lastChunkIndexObjectKeyPartition1 =
        String.format("%s/last_chunk_index.%s-00001.txt", prefix, topicName);
    indexObjectKey =
        String.format("%s/%s/%s-00001-000000000000.index.json", prefix, today, topicName);
    objectContents = getS3FileOutput(bucketName, lastChunkIndexObjectKeyPartition1);
    assertEquals(indexObjectKey + "\n", objectContents);

    // Validate partition 2's index object
    String lastChunkIndexObjectKeyPartition2 =
        String.format("%s/last_chunk_index.%s-00002.txt", prefix, topicName);
    indexObjectKey =
        String.format("%s/%s/%s-00002-000000000000.index.json", prefix, today, topicName);
    objectContents = getS3FileOutput(bucketName, lastChunkIndexObjectKeyPartition2);
    assertEquals(indexObjectKey + "\n", objectContents);

    // Delete the connector
    deleteConnector(connectorName);
  }

  @Test
  public void testSinkWithWallTimeFlushingAndRewind() throws Exception {
    String bucketName = "connect-system-test-wall-time";
    String prefix = "systest";
    String topicName = "system_test_wall_time";
    String connectorName = "s3-sink-wall-time";
    s3.createBucket(bucketName);

    // Create the test topic
    kafkaCluster.createTopic(TopicConfig.withName(topicName).withNumberOfPartitions(1));

    // Define, register, and start the connector
    final long flushIntevalMs = 30 * 1000;
    ConnectorConfiguration connectorConfiguration =
        getConnectorConfiguration(
            connectorName,
            bucketName,
            prefix,
            topicName,
            67108864,
            100 * 1024 * 1024,
            flushIntevalMs);
    kafkaConnectContainer.registerConnector(connectorName, connectorConfiguration);
    kafkaConnectContainer.ensureConnectorTaskState(connectorName, 0, State.RUNNING);

    assertEquals(-1, getConsumerOffset("connect-" + connectorName, topicName, 0));

    // Produce messages to the topic
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
      kafkaCluster.send(SendValues.to(topicName, json));
      sb.append(json).append("\n");
    }
    String firstObjectExpectedContent = sb.toString();
    LocalDate today = LocalDate.now(ZoneOffset.UTC);

    // Define the expected files
    String dataObjectKey =
        String.format("%s/%s/%s-00000-000000000000.gz", prefix, today, topicName);
    String indexObjectKey =
        String.format("%s/%s/%s-00000-000000000000.index.json", prefix, today, topicName);
    String lastChunkIndexObjectKey =
        String.format("%s/last_chunk_index.%s-00000.txt", prefix, topicName);
    List<String> expectedObjects = List.of(indexObjectKey, dataObjectKey, lastChunkIndexObjectKey);

    // Make sure files are produced using wall time flushing
    Awaitility.await()
        .atMost(flushIntevalMs * 3, TimeUnit.MILLISECONDS)
        .until(() -> expectedObjects.stream().allMatch(key -> objectKeyExists(bucketName, key)));

    assertEquals(100, getConsumerOffset("connect-" + connectorName, topicName, 0));

    String objectContents = getS3FileOutput(bucketName, dataObjectKey);
    assertEquals(firstObjectExpectedContent, objectContents);

    sb = new StringBuilder();
    for (int i = 100; i < 200; i++) {
      String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
      kafkaCluster.send(SendValues.to(topicName, json));
      sb.append(json).append("\n");
    }
    String secondObjectExpectedContent = sb.toString();

    String dataObjectKey2 =
        String.format("%s/%s/%s-00000-000000000100.gz", prefix, today, topicName);
    String indexObjectKey2 =
        String.format("%s/%s/%s-00000-000000000100.index.json", prefix, today, topicName);
    List<String> expectedObjects2 =
        List.of(indexObjectKey2, dataObjectKey2, lastChunkIndexObjectKey);

    // Make sure files are produced using wall time flushing
    Awaitility.await()
        .atMost(flushIntevalMs * 3, TimeUnit.MILLISECONDS)
        .until(() -> expectedObjects2.stream().allMatch(key -> objectKeyExists(bucketName, key)));

    assertEquals(200, getConsumerOffset("connect-" + connectorName, topicName, 0));

    objectContents = getS3FileOutput(bucketName, dataObjectKey2);
    assertEquals(secondObjectExpectedContent, objectContents);

    deleteConnector(connectorName);

    for (int i = 200; i < 300; i++) {
      String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
      kafkaCluster.send(SendValues.to(topicName, json));
    }

    s3.deleteObjects(
        new DeleteObjectsRequest(bucketName)
            .withKeys(
                lastChunkIndexObjectKey,
                dataObjectKey,
                indexObjectKey,
                dataObjectKey2,
                indexObjectKey2));
    s3.deleteBucket(bucketName);
    s3.createBucket(bucketName);

    assertTrue(expectedObjects.stream().noneMatch(key -> objectKeyExists(bucketName, key)));
    assertTrue(expectedObjects2.stream().noneMatch(key -> objectKeyExists(bucketName, key)));

    // Rewind the consumer to the first record
    kafkaAdmin
        .alterConsumerGroupOffsets(
            "connect-" + connectorName,
            Map.of(new TopicPartition(topicName, 0), new OffsetAndMetadata(0)))
        .all()
        .get();

    assertEquals(0, getConsumerOffset("connect-" + connectorName, topicName, 0));

    kafkaConnectContainer.registerConnector(connectorName, connectorConfiguration);
    kafkaConnectContainer.ensureConnectorTaskState(connectorName, 0, State.RUNNING);

    // Check files are produced again shortly after the connector restart
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () ->
                Stream.concat(expectedObjects.stream(), expectedObjects2.stream())
                    .allMatch(key -> objectKeyExists(bucketName, key)));

    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> getConsumerOffset("connect-" + connectorName, topicName, 0) == 200);

    // Check same files with same content are produced again
    objectContents = getS3FileOutput(bucketName, dataObjectKey);
    assertEquals(firstObjectExpectedContent, objectContents);

    objectContents = getS3FileOutput(bucketName, dataObjectKey2);
    assertEquals(secondObjectExpectedContent, objectContents);

    deleteConnector(connectorName);
  }

  @Test
  public void testSinkWithBinaryFormat() throws Exception {
    String bucketName = "connect-system-test";
    String prefix = "binsystest";
    String sinkTopicName = "binary-system_test";
    String sinkConnectorName = "s3-sink";
    s3.createBucket(bucketName);

    // Create the test topic
    kafkaCluster.createTopic(TopicConfig.withName(sinkTopicName).withNumberOfPartitions(1));

    // Define, register, and start the connector
    ConnectorConfiguration connectorConfiguration =
        getConnectorConfiguration(sinkConnectorName, bucketName, prefix, sinkTopicName)
            .with("key.converter", "com.spredfast.kafka.connect.s3.AlreadyBytesConverter")
            .with("value.converter", "com.spredfast.kafka.connect.s3.AlreadyBytesConverter");
    kafkaConnectContainer.registerConnector(sinkConnectorName, connectorConfiguration);
    kafkaConnectContainer.ensureConnectorTaskState(sinkConnectorName, 0, State.RUNNING);

    // Produce messages to the topic
    List<String> producedMessages = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
      kafkaCluster.send(SendValues.to(sinkTopicName, json));
      producedMessages.add(json);
    }

    String expectedDataObjectContents = StringUtils.join(producedMessages, "\n");

    // Wait for the last chunk object
    final String lastChunkIndexObjectKeyPartition0 =
        String.format("%s/last_chunk_index.%s-00000.txt", prefix, sinkTopicName);
    Awaitility.await()
        .pollDelay(1, TimeUnit.SECONDS)
        .atMost(60, TimeUnit.SECONDS)
        .until(() -> objectKeyExists(bucketName, lastChunkIndexObjectKeyPartition0));

    // Delete the sink connector
    deleteConnector(sinkConnectorName);

    String sourceTopicName = "test-source-output";
    kafkaCluster.createTopic(TopicConfig.withName(sourceTopicName).withNumberOfPartitions(1));

    String sourceConnectorName = "s3-source";
    ConnectorConfiguration sourceConnectorConfiguration =
        getSourceBinaryConnectorConfiguration(
            sourceConnectorName, bucketName, prefix, sinkTopicName, sourceTopicName);

    kafkaConnectContainer.registerConnector(sourceConnectorName, sourceConnectorConfiguration);
    kafkaConnectContainer.ensureConnectorTaskState(sourceConnectorName, 0, State.RUNNING);
    kafkaCluster.observe(on(sourceTopicName, 100));
    List<String> values =
        kafkaCluster.read(ReadKeyValues.from(sourceTopicName)).stream()
            .map(KeyValue::getValue)
            .collect(Collectors.toList());

    // Delete the connector
    deleteConnector(sourceConnectorName);

    String actualDataObjectContents = StringUtils.join(values, "\n");
    assertEquals(expectedDataObjectContents, actualDataObjectContents);
  }

  private static ConnectorConfiguration getConnectorConfiguration(
      String connectorName, String bucketName, String prefix, String topicName) {
    return ConnectorConfiguration.create()
        .with("name", connectorName)
        .with("connector.class", "com.spredfast.kafka.connect.s3.sink.S3SinkConnector")
        .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
        .with("local.buffer.dir", "/tmp/connect-system-test")
        .with("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter")
        .with("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter")
        .with("internal.key.converter.schemas.enable", true)
        .with("internal.value.converter.schemas.enable", true)
        .with("s3.region", "us-west-2")
        .with("s3.endpoint", "http://localstack:4566")
        .with("s3.bucket", bucketName)
        .with("s3.prefix", prefix)
        .with("s3.path_style", true)
        .with("topics", topicName)
        .with("tasks.max", 1)
        .with("value.converter", "org.apache.kafka.connect.storage.StringConverter");
  }

  private static ConnectorConfiguration getConnectorConfiguration(
      String connectorName,
      String bucketName,
      String prefix,
      String topicName,
      long chunkThreshold,
      long gzipThreshold,
      long flushIntervalMs) {
    return getConnectorConfiguration(connectorName, bucketName, prefix, topicName)
        .with("compressed_block_size", chunkThreshold)
        .with("compressed_file_size", gzipThreshold)
        .with("flush.interval.ms", flushIntervalMs);
  }

  private static ConnectorConfiguration getSourceBinaryConnectorConfiguration(
      String connectorName,
      String bucketName,
      String prefix,
      String sinkTopicName,
      String sourceTopicName) {
    return ConnectorConfiguration.create()
        .with("name", connectorName)
        .with("connector.class", "com.spredfast.kafka.connect.s3.source.S3SourceConnector")
        .with("key.converter", "com.spredfast.kafka.connect.s3.AlreadyBytesConverter")
        .with("value.converter", "com.spredfast.kafka.connect.s3.AlreadyBytesConverter")
        .with("local.buffer.dir", "/tmp/connect-system-test")
        .with("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter")
        .with("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter")
        .with("internal.key.converter.schemas.enable", true)
        .with("internal.value.converter.schemas.enable", true)
        .with("s3.region", "us-west-2")
        .with("s3.endpoint", "http://localstack:4566")
        .with("s3.bucket", bucketName)
        .with("s3.prefix", prefix)
        .with("s3.path_style", true)
        // .with("topics", topicName)
        .with("targetTopic." + sinkTopicName, sourceTopicName)
        .with("tasks.max", 1);
  }

  private void deleteConnector(String connector) {
    kafkaConnectContainer.deleteConnector(connector);
    Awaitility.await()
        .pollDelay(1, TimeUnit.SECONDS)
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> kafkaConnectContainer.getRegisteredConnectors().isEmpty());
  }

  private boolean objectKeyExists(String bucketName, String objectKey) {
    boolean returnValue;

    try {
      returnValue = s3.doesObjectExist(bucketName, objectKey);
    } catch (SdkClientException e) {
      returnValue = false;
    }

    return returnValue;
  }

  private long getConsumerOffset(String groupId, String topic, int partition)
      throws ExecutionException, InterruptedException {
    var offset =
        kafkaAdmin
            .listConsumerGroupOffsets(groupId)
            .partitionsToOffsetAndMetadata()
            .get()
            .get(new TopicPartition(topic, partition));
    return offset == null ? -1 : offset.offset();
  }

  private String getS3FileOutput(String bucketName, String objectKey) throws IOException {
    StringBuilder sb = new StringBuilder();
    S3Object s3Object = s3.getObject(bucketName, objectKey);

    InputStream inputStream;
    if (objectKey.endsWith(".gz")) {
      inputStream = new GzipCompressorInputStream(s3Object.getObjectContent(), true);
    } else {
      inputStream = s3Object.getObjectContent();
    }

    String line;
    BufferedReader br =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    while ((line = br.readLine()) != null) {
      sb.append(line).append("\n");
    }

    inputStream.close();
    return sb.toString();
  }
}
