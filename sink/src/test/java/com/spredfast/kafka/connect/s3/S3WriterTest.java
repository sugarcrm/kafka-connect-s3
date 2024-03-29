package com.spredfast.kafka.connect.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spredfast.kafka.connect.s3.sink.BlockGZIPFileWriter;
import com.spredfast.kafka.connect.s3.sink.S3Writer;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Really basic sanity check testing over the documented use of API. I've not bothered to properly
 * mock result objects etc. as it really only tests how well I can mock S3 API beyond a certain
 * point.
 */
public class S3WriterTest {
  private static final Supplier<String> DATE_SUPPLIER = new CurrentUtcDateSupplier();

  private final String testBucket = "kafka-connect-s3-unit-test";
  private final File tmpDir;

  public S3WriterTest() {

    String tempDir = System.getProperty("java.io.tmpdir");
    String tmpDirPrefix = "S3WriterTest";
    this.tmpDir = new File(tempDir, tmpDirPrefix);

    System.out.println("Temp dir for writer test is: " + tmpDir);
  }

  @Before
  public void setUp() {
    assertTrue(tmpDir.exists() || tmpDir.mkdir());
  }

  private BlockGZIPFileWriter createDummyFiles(long offset, int numRecords) throws Exception {
    BlockGZIPFileWriter writer = new BlockGZIPFileWriter(tmpDir, offset);
    for (int i = 0; i < numRecords; i++) {
      writer.write(String.format("Record %d", i).getBytes(), 1);
    }
    writer.close();
    return writer;
  }

  static class ExpectedRequestParams {
    public String key;
    public String bucket;
    public String content;

    public ExpectedRequestParams(String k, String b) {
      this(k, b, null);
    }

    public ExpectedRequestParams(String k, String b, String content) {
      key = k;
      bucket = b;
      this.content = content;
    }
  }

  private void verifyStringPut(S3Client mock, ExpectedRequestParams[] expected) throws Exception {
    ArgumentCaptor<PutObjectRequest> requestArgumentCaptor =
        ArgumentCaptor.forClass(PutObjectRequest.class);
    ArgumentCaptor<RequestBody> requestBodyArgumentCaptor =
        ArgumentCaptor.forClass(RequestBody.class);
    verify(mock, times(expected.length))
        .putObject(requestArgumentCaptor.capture(), requestBodyArgumentCaptor.capture());

    List<PutObjectRequest> requests = requestArgumentCaptor.getAllValues();
    List<RequestBody> bodies = requestBodyArgumentCaptor.getAllValues();

    for (ExpectedRequestParams e : expected) {
      PutObjectRequest req = requests.remove(0);
      RequestBody body = bodies.remove(0);

      assertEquals(e.bucket, req.bucket());
      assertEquals(e.key, req.key());

      if (e.content != null) {
        ByteArrayOutputStream content = new ByteArrayOutputStream();
        try (InputStream in = body.contentStreamProvider().newStream()) {
          in.transferTo(content);
        }
        assertEquals(e.content, content.toString(StandardCharsets.UTF_8));
      }
    }
  }

  private String getKeyForFilename(
      Layout.Builder layoutBuilder,
      String prefix,
      String topic,
      int partition,
      long startOffset,
      String extension) {
    final TopicPartition topicPartition = new TopicPartition(topic, partition);
    final BlockMetadata blockMetadata = new BlockMetadata(topicPartition, startOffset);
    return String.format("%s/%s%s", prefix, layoutBuilder.buildBlockPath(blockMetadata), extension);
  }

  private String getKeyForIndex(Layout.Builder layoutBuilder, String topic) {
    final TopicPartition topicPartition = new TopicPartition(topic, 0);
    return String.format("%s/%s", "pfx", layoutBuilder.buildIndexPath(topicPartition));
  }

  @Test
  public void testUploadGroupedByDate() throws Exception {
    testUpload(new GroupedByDateLayout(DATE_SUPPLIER));
  }

  @Test
  public void testUploadGroupedByTopic() throws Exception {
    testUpload(new GroupedByTopicLayout(DATE_SUPPLIER));
  }

  private void testUpload(Layout layout) throws Exception {
    S3Client s3Mock = mock(S3Client.class);
    when(s3Mock.putObject(any(Consumer.class), any(Path.class))).thenCallRealMethod();
    when(s3Mock.putObject(any(Consumer.class), any(RequestBody.class))).thenCallRealMethod();
    when(s3Mock.putObject(any(PutObjectRequest.class), any(Path.class))).thenCallRealMethod();

    Layout.Builder layoutBuilder = layout.getBuilder();
    try (BlockGZIPFileWriter fileWriter = createDummyFiles(0, 1000)) {
      S3Writer s3Writer = new S3Writer(testBucket, "pfx", layoutBuilder, s3Mock);
      TopicPartition tp = new TopicPartition("bar", 0);

      String dataKey = getKeyForFilename(layoutBuilder, "pfx", "bar", 0, 0, ".gz");
      String indexKey = getKeyForFilename(layoutBuilder, "pfx", "bar", 0, 0, ".index.json");

      s3Writer.putChunk(
          fileWriter.getDataFile(),
          fileWriter.getIndexFile(),
          new BlockMetadata(tp, fileWriter.getStartOffset()));

      // Verify it wrote data and index files as well as the cursor file with index key as the
      // content
      verifyStringPut(
          s3Mock,
          new ExpectedRequestParams[] {
            new ExpectedRequestParams(dataKey, testBucket),
            new ExpectedRequestParams(indexKey, testBucket),
            new ExpectedRequestParams(getKeyForIndex(layoutBuilder, "bar"), testBucket, indexKey)
          });
    }
  }

  @Test
  public void testFetchOffsetNewTopicGroupedByDate() throws Exception {
    testFetchOffsetNewTopic(new GroupedByDateLayout(DATE_SUPPLIER));
  }

  @Test
  public void testFetchOffsetNewTopicGroupedByTopic() throws Exception {
    testFetchOffsetNewTopic(new GroupedByTopicLayout(DATE_SUPPLIER));
  }

  private void testFetchOffsetNewTopic(Layout layout) throws Exception {
    S3Client s3Mock = mock(S3Client.class);
    when(s3Mock.getObjectAsBytes(any(Consumer.class))).thenCallRealMethod();

    Layout.Builder layoutBuilder = layout.getBuilder();
    S3Writer s3Writer = new S3Writer(testBucket, "pfx", layoutBuilder, s3Mock);

    GetObjectRequest expectedRequest = getObjectRequest(getKeyForIndex(layoutBuilder, "new_topic"));

    // Non existing topic should return 0 offset
    // Since the file won't exist code will expect NoSuchKeyException to be thrown
    when(s3Mock.getObjectAsBytes(eq(expectedRequest)))
        .thenThrow(NoSuchKeyException.builder().build());

    TopicPartition tp = new TopicPartition("new_topic", 0);
    long offset = s3Writer.fetchOffset(tp);
    assertEquals(0, offset);
    verify(s3Mock, times(1)).getObjectAsBytes(eq(expectedRequest));
  }

  @Test
  public void testFetchOffsetExistingTopicGroupedByDate() throws Exception {
    testFetchOffsetExistingTopic(new GroupedByDateLayout(DATE_SUPPLIER));
  }

  @Test
  public void testFetchOffsetExistingTopicGroupedByTopic() throws Exception {
    testFetchOffsetExistingTopic(new GroupedByTopicLayout(DATE_SUPPLIER));
  }

  private void testFetchOffsetExistingTopic(Layout layout) throws Exception {
    S3Client s3Mock = mock(S3Client.class);
    when(s3Mock.getObjectAsBytes(any(Consumer.class))).thenCallRealMethod();

    Layout.Builder layoutBuilder = layout.getBuilder();
    S3Writer s3Writer = new S3Writer(testBucket, "pfx", layoutBuilder, s3Mock);
    // Existing topic should return correct offset
    // We expect 2 fetches, one for the cursor file
    // and second for the index file itself
    String indexKey = getKeyForFilename(layoutBuilder, "pfx", "bar", 0, 10042, ".index.json");

    String barKey = getKeyForIndex(layoutBuilder, "bar");

    when(s3Mock.getObjectAsBytes(eq(getObjectRequest(barKey))))
        .thenReturn(
            ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), indexKey.getBytes()));

    String indexContent =
        "{\"chunks\":["
            // Assume 10 byte records, split into 3 chunks for same of checking the logic
            // about next offset
            // We expect next offset to be 12031 + 34
            + "{\"first_record_offset\":10042,\"num_records\":1000,\"byte_offset\":0,\"byte_length\":10000},"
            + "{\"first_record_offset\":11042,\"num_records\":989,\"byte_offset\":10000,\"byte_length\":9890},"
            + "{\"first_record_offset\":12031,\"num_records\":34,\"byte_offset\":19890,\"byte_length\":340}"
            + "]}";

    when(s3Mock.getObjectAsBytes(eq(getObjectRequest(indexKey))))
        .thenReturn(
            ResponseBytes.fromByteArray(
                GetObjectResponse.builder().build(), indexContent.getBytes()));

    TopicPartition tp = new TopicPartition("bar", 0);
    long offset = s3Writer.fetchOffset(tp);
    assertEquals(12031 + 34, offset);
    verify(s3Mock).getObjectAsBytes(eq(getObjectRequest(barKey)));
    verify(s3Mock).getObjectAsBytes(eq(getObjectRequest(indexKey)));
  }

  private GetObjectRequest getObjectRequest(String key) {
    return GetObjectRequest.builder().bucket(testBucket).key(key).build();
  }
}
