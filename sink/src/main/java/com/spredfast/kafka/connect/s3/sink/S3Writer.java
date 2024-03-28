package com.spredfast.kafka.connect.s3.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.spredfast.kafka.connect.s3.BlockMetadata;
import com.spredfast.kafka.connect.s3.Layout;
import com.spredfast.kafka.connect.s3.json.ChunkDescriptor;
import com.spredfast.kafka.connect.s3.json.ChunksIndex;
import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * S3Writer provides necessary operations over S3 to store files and retrieve Last commit offsets
 * for a TopicPartition.
 *
 * <p>Maybe one day we could make this an interface and support pluggable storage backends... but
 * for now it's just to keep things simpler to test.
 */
public class S3Writer {
  private static final Logger log = LoggerFactory.getLogger(S3SinkTask.class);
  private final ObjectReader reader = new ObjectMapper().readerFor(ChunksIndex.class);
  private final String keyPrefix;
  private final String bucket;
  private final Layout.Builder layoutBuilder;
  private final S3Client s3Client;

  public S3Writer(
      String bucket, String keyPrefix, Layout.Builder layoutBuilder, S3Client s3Client) {
    if (!keyPrefix.isEmpty() && !keyPrefix.endsWith("/")) {
      keyPrefix += "/";
    }
    this.keyPrefix = keyPrefix;
    this.bucket = bucket;
    this.layoutBuilder = layoutBuilder;
    this.s3Client = s3Client;
  }

  public void putChunk(File dataFile, File indexFile, BlockMetadata metadata) throws IOException {

    // Build the base key once to make sure that both the data and the index keys always fall under
    // the same date.
    final String baseKey = keyPrefix + layoutBuilder.buildBlockPath(metadata);

    // Put data file then index, then finally update/create the last_index_file marker
    final String dataObjectKey = baseKey + ".gz";
    final String indexObjectKey = baseKey + ".index.json";

    try {
      s3Client.putObject(putKey(dataObjectKey), dataFile.toPath());
      log.debug("uploaded {} object to s3", dataObjectKey);
      s3Client.putObject(putKey(indexObjectKey), indexFile.toPath());
      log.debug("uploaded {} object to s3", indexObjectKey);
    } catch (Exception e) {
      throw new IOException("Failed to upload to S3", e);
    }

    this.updateCursorFile(indexObjectKey, metadata.getTopicPartition());
  }

  public long fetchOffset(TopicPartition tp) throws IOException {
    // See if cursor file exists
    String indexFileKey;
    String cursorFileKey = getCursorFileKey(tp);

    try {
      indexFileKey = s3Client.getObjectAsBytes(getKey(cursorFileKey)).asUtf8String();
    } catch (NoSuchKeyException e) {
      // Topic partition has no data in S3, start from beginning
      return 0;
    } catch (Exception e) {
      throw new IOException("Failed to fetch or read cursor file", e);
    }

    // Now fetch last written index file...
    try {
      byte[] indexJSON = s3Client.getObjectAsBytes(getKey(indexFileKey)).asByteArray();
      return getNextOffsetFromIndexFileContents(indexJSON);
    } catch (Exception e) {
      throw new IOException("Failed to fetch or parse last index file", e);
    }
  }

  private long getNextOffsetFromIndexFileContents(byte[] indexJSON) throws IOException {
    ChunksIndex index = reader.readValue(indexJSON);
    ChunkDescriptor lastChunk = index.chunks.get(index.chunks.size() - 1);
    return lastChunk.first_record_offset + lastChunk.num_records;
  }

  private String getCursorFileKey(TopicPartition tp) {
    return keyPrefix + layoutBuilder.buildIndexPath(tp);
  }

  private void updateCursorFile(String lastIndexFileKey, TopicPartition tp) throws IOException {
    String cursorFileKey = getCursorFileKey(tp);
    try {
      s3Client.putObject(putKey(cursorFileKey), RequestBody.fromString(lastIndexFileKey));
    } catch (Exception ex) {
      throw new IOException("Failed to update cursor file", ex);
    }
  }

  private Consumer<PutObjectRequest.Builder> putKey(String key) {
    return builder -> builder.bucket(bucket).key(key);
  }

  private Consumer<GetObjectRequest.Builder> getKey(String key) {
    return builder -> builder.bucket(bucket).key(key);
  }
}
