package com.spredfast.kafka.connect.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.spredfast.kafka.connect.s3.sink.BlockGZIPFileWriter;
import com.spredfast.kafka.connect.s3.source.S3FilesReader;
import com.spredfast.kafka.connect.s3.source.S3Offset;
import com.spredfast.kafka.connect.s3.source.S3Partition;
import com.spredfast.kafka.connect.s3.source.S3SourceConfig;
import com.spredfast.kafka.connect.s3.source.S3SourceRecord;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/** Covers S3 and reading raw byte records. Closer to an integration test. */
public class S3FilesReaderTest {
  private static final DateSupplier DATE_SUPPLIER = new DateSupplier();

  @Test
  public void testReadingBytesFromS3GroupedByDate() throws IOException, NoSuchFieldException {
    testReadingBytesFromS3(new GroupedByDateLayout(DATE_SUPPLIER));
  }

  @Test
  public void testReadingBytesFromS3GroupedByTopic() throws IOException, NoSuchFieldException {
    testReadingBytesFromS3(new GroupedByTopicLayout(DATE_SUPPLIER));
  }

  private void testReadingBytesFromS3(Layout layout) throws IOException, NoSuchFieldException {
    final Path dir = Files.createTempDirectory("s3FilesReaderTest");
    givenSomeData(dir, layout.getBuilder());

    final S3Client client = givenAMockS3Client(dir);

    List<String> results = whenTheRecordsAreRead(client, layout, true, 3);

    thenTheyAreFilteredAndInOrder(results);
  }

  @Test
  public void testExcludingByMessageKeyGroupedByDate() throws IOException, NoSuchFieldException {
    testExcludingByMessageKey(new GroupedByDateLayout(DATE_SUPPLIER));
  }

  @Test
  public void testExcludingByMessageKeyGroupedByTopic() throws IOException, NoSuchFieldException {
    testExcludingByMessageKey(new GroupedByTopicLayout(DATE_SUPPLIER));
  }

  private void testExcludingByMessageKey(Layout layout) throws IOException, NoSuchFieldException {
    final Path dir = Files.createTempDirectory("s3FilesReaderTest");
    givenSomeData(dir, layout.getBuilder());

    final S3Client client = givenAMockS3Client(dir);

    List<String> results = whenTheRecordsAreRead(client, layout, Arrays.asList("1-0", "ololo"));
    assertEquals(Arrays.asList("key0-0=value0-0", "key1-1=value1-1"), results);
  }

  @Test
  public void testReadingBytesFromS3MultiPartitionGroupedByDate()
      throws IOException, NoSuchFieldException {
    testReadingBytesFromS3MultiPartition(new GroupedByDateLayout(DATE_SUPPLIER));
  }

  @Test
  public void testReadingBytesFromS3MultiPartitionGroupedByTopic()
      throws IOException, NoSuchFieldException {
    testReadingBytesFromS3MultiPartition(new GroupedByTopicLayout(DATE_SUPPLIER));
  }

  private void testReadingBytesFromS3MultiPartition(Layout layout)
      throws IOException, NoSuchFieldException {
    // scenario: multiple partition files at the end of a listing, page size >  # of files
    // do we read all of them?
    final Path dir = Files.createTempDirectory("s3FilesReaderTest");
    givenASingleDayWithManyPartitions(dir, layout.getBuilder());

    final S3Client client = givenAMockS3Client(dir);

    List<String> results = whenTheRecordsAreRead(client, layout, true, 10);

    thenTheyAreFilteredAndInOrder(results);
  }

  @Test
  public void testReadingBytesFromS3WithOffsetsGroupedByDate()
      throws IOException, NoSuchFieldException {
    testReadingBytesFromS3WithOffsets(new GroupedByDateLayout(DATE_SUPPLIER));
  }

  @Test
  public void testReadingBytesFromS3WithOffsetsGroupedByTopic()
      throws IOException, NoSuchFieldException {
    testReadingBytesFromS3WithOffsets(new GroupedByTopicLayout(DATE_SUPPLIER));
  }

  private void testReadingBytesFromS3WithOffsets(Layout layout)
      throws IOException, NoSuchFieldException {
    final Path dir = Files.createTempDirectory("s3FilesReaderTest");
    givenSomeData(dir, layout.getBuilder());

    final S3Client client = givenAMockS3Client(dir);

    List<String> results =
        whenTheRecordsAreRead(
            givenAReaderWithOffsets(
                client,
                layout.getParser(),
                getKeyForFilename(
                    layout.getBuilder(), "2015-12-31", "prefix", "topic", 3, 1, ".gz"),
                5L,
                "00003"));

    assertEquals(
        Arrays.asList(
            "willbe=skipped5",
            "willbe=skipped6",
            "willbe=skipped7",
            "willbe=skipped8",
            "willbe=skipped9"),
        results);
  }

  @Test
  public void testReadingBytesFromS3WithOffsetsAtEndOfFileGroupedByDate()
      throws IOException, NoSuchFieldException {
    testReadingBytesFromS3WithOffsetsAtEndOfFile(new GroupedByDateLayout(DATE_SUPPLIER));
  }

  @Test
  public void testReadingBytesFromS3WithOffsetsAtEndOfFileGroupedByTopic()
      throws IOException, NoSuchFieldException {
    testReadingBytesFromS3WithOffsetsAtEndOfFile(new GroupedByTopicLayout(DATE_SUPPLIER));
  }

  private void testReadingBytesFromS3WithOffsetsAtEndOfFile(Layout layout)
      throws IOException, NoSuchFieldException {
    final Path dir = Files.createTempDirectory("s3FilesReaderTest");
    givenSomeData(dir, layout.getBuilder());

    final S3Client client = givenAMockS3Client(dir);

    // this file will be skipped
    List<String> results =
        whenTheRecordsAreRead(
            givenAReaderWithOffsets(
                client,
                layout.getParser(),
                getKeyForFilename(
                    layout.getBuilder(), "2015-12-30", "prefix", "topic", 3, 0, ".gz"),
                1L,
                "00003"));

    assertEquals(
        Arrays.asList(
            "willbe=skipped1",
            "willbe=skipped2",
            "willbe=skipped3",
            "willbe=skipped4",
            "willbe=skipped5",
            "willbe=skipped6",
            "willbe=skipped7",
            "willbe=skipped8",
            "willbe=skipped9"),
        results);
  }

  private S3FilesReader givenAReaderWithOffsets(
      S3Client client,
      Layout.Parser layoutParser,
      String marker,
      long nextOffset,
      final String partition) {
    Map<S3Partition, S3Offset> offsets = new HashMap<>();
    int partInt = Integer.valueOf(partition, 10);
    offsets.put(
        S3Partition.from("bucket", "prefix", "topic", partInt),
        S3Offset.from(
            marker,
            nextOffset
                - 1 /* an S3 offset is the last record processed, so go back 1 to consume next */));
    return new S3FilesReader(
        new S3SourceConfig(
            "bucket", "prefix", 1, null, S3FilesReader.InputFilter.GUNZIP, p -> partInt == p, null),
        client,
        offsets,
        layoutParser,
        () -> new BytesRecordReader(true));
  }

  @Test
  public void testReadingBytesFromS3WithoutKeysGroupedByDate()
      throws IOException, NoSuchFieldException {
    testReadingBytesFromS3WithoutKeys(new GroupedByDateLayout(DATE_SUPPLIER));
  }

  @Test
  public void testReadingBytesFromS3WithoutKeysGroupedByTopic()
      throws IOException, NoSuchFieldException {
    testReadingBytesFromS3WithoutKeys(new GroupedByTopicLayout(DATE_SUPPLIER));
  }

  private void testReadingBytesFromS3WithoutKeys(Layout layout)
      throws IOException, NoSuchFieldException {
    final Path dir = Files.createTempDirectory("s3FilesReaderTest");
    givenSomeData(dir, layout.getBuilder(), false);

    final S3Client client = givenAMockS3Client(dir);

    List<String> results = whenTheRecordsAreRead(client, layout, false);

    theTheyAreInOrder(results);
  }

  void theTheyAreInOrder(List<String> results) {
    List<String> expected = Arrays.asList("value0-0", "value1-0", "value1-1");
    assertEquals(expected, results);
  }

  private void thenTheyAreFilteredAndInOrder(List<String> results) {
    List<String> expected = Arrays.asList("key0-0=value0-0", "key1-0=value1-0", "key1-1=value1-1");
    assertEquals(expected, results);
  }

  private List<String> whenTheRecordsAreRead(
      S3Client client, Layout layout, boolean fileIncludesKeys) {
    return whenTheRecordsAreRead(client, layout, fileIncludesKeys, 1);
  }

  private List<String> whenTheRecordsAreRead(
      S3Client client, Layout layout, List<String> messageKeyExcludeList) {
    S3SourceConfig config =
        new S3SourceConfig(
            "bucket",
            "prefix",
            3,
            buildStartMarker(layout.getBuilder(), "prefix", "topic", "2016-01-01"),
            S3FilesReader.InputFilter.GUNZIP,
            null,
            messageKeyExcludeList);
    S3FilesReader reader =
        new S3FilesReader(
            config, client, null, layout.getParser(), () -> new BytesRecordReader(true));
    return whenTheRecordsAreRead(reader);
  }

  private List<String> whenTheRecordsAreRead(
      S3Client client, Layout layout, boolean fileIncludesKeys, int pageSize) {
    S3FilesReader reader =
        new S3FilesReader(
            new S3SourceConfig(
                "bucket",
                "prefix",
                pageSize,
                buildStartMarker(layout.getBuilder(), "prefix", "topic", "2016-01-01"),
                S3FilesReader.InputFilter.GUNZIP,
                null,
                null),
            client,
            null,
            layout.getParser(),
            () -> new BytesRecordReader(fileIncludesKeys));
    return whenTheRecordsAreRead(reader);
  }

  private String buildStartMarker(
      Layout.Builder layoutBuilder, String prefix, String topic, String date) {
    return Path.of(getKeyForFilename(layoutBuilder, date, prefix, topic, 0, 0, ".gz"))
        .getParent()
        .toString();
  }

  private List<String> whenTheRecordsAreRead(S3FilesReader reader) {
    List<String> results = new ArrayList<>();
    for (S3SourceRecord record : reader) {
      results.add(
          (record.key() == null ? "" : new String(record.key()) + "=")
              + new String(record.value()));
    }
    return results;
  }

  private S3Client givenAMockS3Client(final Path dir) throws NoSuchFieldException {
    final S3Client client = mock(S3Client.class, withSettings().verboseLogging());

    when(client.listObjectsV2(any(Consumer.class))).thenCallRealMethod();
    when(client.getObject(any(Consumer.class))).thenCallRealMethod();

    when(client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenAnswer(
            new Answer<ListObjectsV2Response>() {
              @Override
              public ListObjectsV2Response answer(InvocationOnMock invocationOnMock)
                  throws Throwable {
                final ListObjectsV2Request req =
                    (ListObjectsV2Request) invocationOnMock.getArguments()[0];
                ListObjectsV2Response.Builder listing = ListObjectsV2Response.builder();

                final Set<File> files = new TreeSet<>();
                Files.walkFileTree(
                    dir,
                    new SimpleFileVisitor<Path>() {
                      @Override
                      public FileVisitResult preVisitDirectory(
                          Path toCheck, BasicFileAttributes attrs) throws IOException {
                        if (toCheck.startsWith(dir)) {
                          return FileVisitResult.CONTINUE;
                        }
                        return FileVisitResult.SKIP_SUBTREE;
                      }

                      @Override
                      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                          throws IOException {
                        String key = key(file.toFile());
                        if (req.continuationToken() != null) {
                          if (key.compareTo(req.continuationToken()) > 0) {
                            files.add(file.toFile());
                          }
                        } else if (req.startAfter() != null) {
                          if (key.compareTo(req.startAfter()) > 0) {
                            files.add(file.toFile());
                          }
                        } else {
                          files.add(file.toFile());
                        }
                        return FileVisitResult.CONTINUE;
                      }
                    });

                List<S3Object> summaries = new ArrayList<>();
                int count = 0;
                for (File file : files) {
                  if (count++ < req.maxKeys()) {
                    String key = key(file);
                    listing.nextContinuationToken(key);
                    summaries.add(S3Object.builder().key(key).build());
                  } else {
                    break;
                  }
                }

                listing.maxKeys(req.maxKeys());

                listing.contents(summaries);
                listing.isTruncated(files.size() > req.maxKeys());

                return listing.build();
              }

              private String key(File file) {
                return file.getAbsolutePath()
                    .substring(dir.toAbsolutePath().toString().length() + 1);
              }
            });

    when(client.getObject(any(GetObjectRequest.class)))
        .thenAnswer(
            (Answer<ResponseInputStream<GetObjectResponse>>)
                invocationOnMock -> {
                  String key = invocationOnMock.getArgument(0, GetObjectRequest.class).key();
                  return getFile(key, dir);
                });
    return client;
  }

  ResponseInputStream<GetObjectResponse> getFile(String key, Path dir)
      throws FileNotFoundException {
    File file = new File(dir.toString(), key);
    return new ResponseInputStream<GetObjectResponse>(
        GetObjectResponse.builder().build(), new FileInputStream(file));
  }

  private void givenASingleDayWithManyPartitions(Path dir, Layout.Builder layoutBuilder)
      throws IOException {
    givenASingleDayWithManyPartitions(dir, layoutBuilder, true);
  }

  private void givenASingleDayWithManyPartitions(
      Path dir, Layout.Builder layoutBuilder, boolean includeKeys) throws IOException {
    try (BlockGZIPFileWriter p0 = new BlockGZIPFileWriter(dir.toFile(), 0, 512);
        BlockGZIPFileWriter p1 = new BlockGZIPFileWriter(dir.toFile(), 0, 512); ) {
      write(p0, "key0-0".getBytes(), "value0-0".getBytes(), includeKeys);
      upload(p0, dir, layoutBuilder, "2016-01-01", 0);

      write(p1, "key1-0".getBytes(), "value1-0".getBytes(), includeKeys);
      write(p1, "key1-1".getBytes(), "value1-1".getBytes(), includeKeys);
      upload(p1, dir, layoutBuilder, "2016-01-01", 1);
    }
  }

  private void givenSomeData(Path dir, Layout.Builder layoutBuilder) throws IOException {
    givenSomeData(dir, layoutBuilder, true);
  }

  private void givenSomeData(Path dir, Layout.Builder layoutBuilder, boolean includeKeys)
      throws IOException {
    try (BlockGZIPFileWriter writer1 = new BlockGZIPFileWriter(dir.toFile(), 0, 512);
        BlockGZIPFileWriter writer2 = new BlockGZIPFileWriter(dir.toFile(), 1, 512);
        BlockGZIPFileWriter writer3 = new BlockGZIPFileWriter(dir.toFile(), 0, 512);
        BlockGZIPFileWriter writer4 = new BlockGZIPFileWriter(dir.toFile(), 0, 512); ) {
      write(writer1, "willbe".getBytes(), "skipped0".getBytes(), includeKeys);
      upload(writer1, dir, layoutBuilder, "2015-12-30", 3);

      for (int i = 1; i < 10; i++) {
        write(writer2, "willbe".getBytes(), ("skipped" + i).getBytes(), includeKeys);
      }
      upload(writer2, dir, layoutBuilder, "2015-12-31", 3);

      write(writer3, "key0-0".getBytes(), "value0-0".getBytes(), includeKeys);
      upload(writer3, dir, layoutBuilder, "2016-01-01", 0);

      write(writer4, "key1-0".getBytes(), "value1-0".getBytes(), includeKeys);
      write(writer4, "key1-1".getBytes(), "value1-1".getBytes(), includeKeys);
      upload(writer4, dir, layoutBuilder, "2016-01-02", 1);
    }
  }

  private String getKeyForFilename(
      Layout.Builder layoutBuilder,
      String date,
      String prefix,
      String topic,
      int partition,
      long startOffset,
      String extension) {
    DATE_SUPPLIER.set(date);
    final TopicPartition topicPartition = new TopicPartition(topic, partition);
    final BlockMetadata blockMetadata = new BlockMetadata(topicPartition, startOffset);
    return String.format("%s/%s%s", prefix, layoutBuilder.buildBlockPath(blockMetadata), extension);
  }

  private void write(BlockGZIPFileWriter writer, byte[] key, byte[] value, boolean includeKeys)
      throws IOException {
    writer.write(
        new ByteLengthFormat(includeKeys).newWriter().write(new ProducerRecord<>("", key, value)),
        1);
  }

  private void upload(
      BlockGZIPFileWriter writer,
      Path dir,
      Layout.Builder layoutBuilder,
      String date,
      int partition)
      throws IOException {
    writer.close();
    rename(
        writer.getDataFile(), dir, layoutBuilder, date, partition, writer.getStartOffset(), ".gz");
    rename(
        writer.getIndexFile(),
        dir,
        layoutBuilder,
        date,
        partition,
        writer.getStartOffset(),
        ".index.json");
  }

  private void rename(
      File file,
      Path dir,
      Layout.Builder layoutBuilder,
      String date,
      int partition,
      long startOffset,
      String extension) {
    final String key =
        getKeyForFilename(
            layoutBuilder, date, "prefix", "topic", partition, startOffset, extension);
    final File dest = new File(dir.toFile(), key);
    final File parent = dest.getParentFile();
    assertTrue(parent.exists() || parent.mkdirs());
    assertTrue(file.renameTo(dest));
  }

  private static class DateSupplier implements Supplier<String> {
    private String date;

    @Override
    public String get() {
      return date;
    }

    public void set(String date) {
      this.date = date;
    }
  }
}
