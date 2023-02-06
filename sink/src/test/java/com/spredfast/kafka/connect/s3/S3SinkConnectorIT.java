package com.spredfast.kafka.connect.s3;

import static org.junit.Assert.assertEquals;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import net.mguenther.kafka.junit.ExternalKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.SendKeyValues;
import net.mguenther.kafka.junit.SendValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.awaitility.Awaitility;
import org.junit.After;
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

public class S3SinkConnectorIT {

	private static final Logger LOGGER = LoggerFactory.getLogger(S3SinkConnectorIT.class);

	private static final Network network = Network.newNetwork();

	private AmazonS3 s3;

	private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))
		.withNetwork(network);

	public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.11.3"))
		.withNetworkAliases("localstack")
		.withNetwork(network)
		.withServices(Service.S3);

	public static DebeziumContainer kafkaConnectContainer = DebeziumContainer.latestStable()
		.withFileSystemBind("build/libs", "/kafka/connect/s3-sink-connector")
		.withNetwork(network)
		.withKafka(kafkaContainer)
		.withLogConsumer(new Slf4jLogConsumer(LOGGER))
		.withEnv("OFFSET_FLUSH_INTERVAL_MS", "3000")
		.withEnv("CONNECT_CONSUMER_METADATA_MAX_AGE_MS", "1000")
		.dependsOn(kafkaContainer);

	private ExternalKafkaCluster kafkaCluster = ExternalKafkaCluster.at(kafkaContainer.getBootstrapServers());

	@BeforeClass
	public static void startContainers() {
		Startables.deepStart(Stream.of(
			kafkaContainer, localStackContainer, kafkaConnectContainer)).join();
	}

	@Before
	public void setup() {
		s3 = AmazonS3ClientBuilder
			.standard()
			.withEndpointConfiguration(
				new AwsClientBuilder.EndpointConfiguration(
					localStackContainer.getEndpointOverride(Service.S3).toString(),
					localStackContainer.getRegion()
				)
			)
			.withCredentials(
				new AWSStaticCredentialsProvider(
					new BasicAWSCredentials(localStackContainer.getAccessKey(), localStackContainer.getSecretKey())
				)
			)
			.build();

		kafkaCluster = ExternalKafkaCluster.at(kafkaContainer.getBootstrapServers());
	}

	@After
	public void tearDown() {
		localStackContainer.close();
		kafkaConnectContainer.close();
		kafkaContainer.close();
	}

	@Test
	public void shouldSinkEvents() throws Exception {
		String bucketName = "connect-system-test";
		String prefix = "systest";
		String topicName = "system_test";
		s3.createBucket(bucketName);

		// Create the test topic
		kafkaCluster.createTopic(TopicConfig.withName(topicName).withNumberOfPartitions(1));

		// Define, register, and start the connector
		ConnectorConfiguration connectorConfiguration = getConnectorConfiguration(bucketName, prefix, topicName);
		kafkaConnectContainer.registerConnector("s3-sink", connectorConfiguration);
		kafkaConnectContainer.ensureConnectorTaskState("s3-sink", 0, State.RUNNING);

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
		final String lastChunkIndexObjectKeyPartition0 = prefix + "/last_chunk_index.system_test-00000.txt";
		Awaitility.await()
			.pollDelay(1, TimeUnit.SECONDS)
			.atMost(10, TimeUnit.SECONDS)
			.until(() -> objectKeyExists(bucketName, lastChunkIndexObjectKeyPartition0));

		// Validate partition 0's last chunk object
		String indexObjectKey = String.format("%s/%s/system_test-00000-000000000000.index.json", prefix, today);
		String objectContents = getS3FileOutput(bucketName, lastChunkIndexObjectKeyPartition0);
		assertEquals(indexObjectKey + "\n", objectContents);

		// Validate partition 0's data object
		String dataObjectKey = String.format("%s/%s/system_test-00000-000000000000.gz", prefix, today);
		objectContents = getS3FileOutput(bucketName, dataObjectKey);
		assertEquals(expectedDataObjectContents, objectContents);

		// Validate partition 0's index object
		String expectedIndexObjectContents = "{\"chunks\":[{\"byte_length_uncompressed\":3190,\"num_records\":100,\"byte_length\":283,\"byte_offset\":0,\"first_record_offset\":0}]}\n";
		objectContents = getS3FileOutput(bucketName, indexObjectKey);
		assertEquals(expectedIndexObjectContents, objectContents);

		// Delete the connector
		deleteConnector("s3-sink");

		// Produce messages while the connector is not running
		sb.setLength(0);
		for (int i = 100; i < 200; i++) {
			String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
			kafkaCluster.send(SendValues.to(topicName, json));
			sb.append(json).append("\n");
		}
		expectedDataObjectContents = sb.toString();

		// Redeploy the connector
		kafkaConnectContainer.registerConnector("s3-sink", connectorConfiguration);
		kafkaConnectContainer.ensureConnectorTaskState("s3-sink", 0, State.RUNNING);

		// Await for the new index object to be written
		String indexObjectKey2 = String.format("%s/%s/system_test-00000-000000000100.index.json", prefix, today);
		Awaitility.await()
			.pollDelay(1, TimeUnit.SECONDS)
			.atMost(10, TimeUnit.SECONDS)
			.until(() -> objectKeyExists(bucketName, indexObjectKey2));

		// Validate partition 0's last chunk object was updated
		objectContents = getS3FileOutput(bucketName, lastChunkIndexObjectKeyPartition0);
		assertEquals(indexObjectKey2 + "\n", objectContents);

		// Validate partition 0's data object
		dataObjectKey = String.format("%s/%s/system_test-00000-000000000100.gz", prefix, today);
		objectContents = getS3FileOutput(bucketName, dataObjectKey);
		assertEquals(expectedDataObjectContents, objectContents);

		// Validate partition 0's index object
		expectedIndexObjectContents = "{\"chunks\":[{\"byte_length_uncompressed\":3300,\"num_records\":100,\"byte_length\":278,\"byte_offset\":0,\"first_record_offset\":100}]}\n";
		objectContents = getS3FileOutput(bucketName, indexObjectKey2);
		assertEquals(expectedIndexObjectContents, objectContents);

		// Increase the partitions from 1 to 3
		kafkaContainer.execInContainer("kafka-topics","--bootstrap-server", "localhost:9092", "--alter", "--topic", "system_test", "--partitions", "3");
		final int partitionCount = 3;

		// Restart the connector to pick up the partition changes
		deleteConnector("s3-sink");
		kafkaConnectContainer.registerConnector("s3-sink", connectorConfiguration);
		kafkaConnectContainer.ensureConnectorTaskState("s3-sink", 0, State.RUNNING);

		for (int i = 200; i < 300; i++) {
			String json = String.format("{{\"foo\": \"bar\", \"counter\": %d}}", i);
			kafkaCluster.send(SendKeyValues.to(topicName, Collections.singletonList(new KeyValue<>(String.valueOf(i), json))));
		}

		// Define the expected files
		List<String> expectedObjects = List.of(
			String.format("%s/%s/system_test-00000-000000000200.gz", prefix, today),
			String.format("%s/%s/system_test-00000-000000000200.index.json", prefix, today),
			String.format("%s/%s/system_test-00001-000000000000.gz", prefix, today),
			String.format("%s/%s/system_test-00001-000000000000.index.json", prefix, today),
			String.format("%s/%s/system_test-00002-000000000000.gz", prefix, today),
			String.format("%s/%s/system_test-00002-000000000000.index.json", prefix, today),
			String.format("%s/last_chunk_index.system_test-00000.txt", prefix, today),
			String.format("%s/last_chunk_index.system_test-00001.txt", prefix, today),
			String.format("%s/last_chunk_index.system_test-00002.txt", prefix, today)
		);

		// Await for all the files to be produced by all tasks
		Awaitility.await()
			.pollDelay(1, TimeUnit.SECONDS)
			.atMost(10, TimeUnit.SECONDS)
			.until(() -> expectedObjects.stream().allMatch(key -> objectKeyExists(bucketName, key)));

		// Validate partition 0's index object
		indexObjectKey = String.format("%s/%s/system_test-00000-000000000200.index.json", prefix, today);
		objectContents = getS3FileOutput(bucketName, lastChunkIndexObjectKeyPartition0);
		assertEquals(indexObjectKey + "\n", objectContents);

		// Validate partition 0's index object
		String lastChunkIndexObjectKeyPartition1 = prefix + "/last_chunk_index.system_test-00001.txt";
		indexObjectKey = String.format("%s/%s/system_test-00001-000000000000.index.json", prefix, today);
		objectContents = getS3FileOutput(bucketName, lastChunkIndexObjectKeyPartition1);
		assertEquals(indexObjectKey + "\n", objectContents);

		// Validate partition 0's index object
		String lastChunkIndexObjectKeyPartition2 = prefix + "/last_chunk_index.system_test-00002.txt";
		indexObjectKey = String.format("%s/%s/system_test-00002-000000000000.index.json", prefix, today);
		objectContents = getS3FileOutput(bucketName, lastChunkIndexObjectKeyPartition2);
		assertEquals(indexObjectKey + "\n", objectContents);
	}

	private static ConnectorConfiguration getConnectorConfiguration(String bucketName, String prefix, String topicName) {
		ConnectorConfiguration connector = ConnectorConfiguration.create()
			.with("name", "s3-sink")
			.with("connector.class", "com.spredfast.kafka.connect.s3.sink.S3SinkConnector")
			.with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
			.with("local.buffer.dir", "/tmp/connect-system-test")
			.with("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter")
			.with("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter")
			.with("internal.key.converter.schemas.enable", true)
			.with("internal.value.converter.schemas.enable", true)
			.with("s3.bucket", bucketName)
			.with("aws.accessKeyId", localStackContainer.getAccessKey())
			.with("aws.secretAccessKey", localStackContainer.getSecretKey())
			.with("region", localStackContainer.getRegion())
			.with("s3.path_style", true)
			.with("s3.prefix", prefix)
			.with("s3.endpoint", "http://localstack:4566")
			.with("topics", topicName)
			.with("tasks.max", 1)
			.with("value.converter", "org.apache.kafka.connect.storage.StringConverter");
		return connector;
	}

	private void deleteConnector(String connector) {
		kafkaConnectContainer.deleteConnector(connector);
		Awaitility.await()
			.pollDelay(1, TimeUnit.SECONDS)
			.atMost(5, TimeUnit.SECONDS)
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
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		while ((line = br.readLine()) != null) {
			sb.append(line).append("\n");
		}

		inputStream.close();
		return sb.toString();
	}
}
