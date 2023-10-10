package com.spredfast.kafka.connect.s3.sink;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import com.amazonaws.services.s3.AmazonS3;
import com.spredfast.kafka.connect.s3.AlreadyBytesConverter;
import com.spredfast.kafka.connect.s3.BlockMetadata;
import com.spredfast.kafka.connect.s3.Configure;
import com.spredfast.kafka.connect.s3.Constants;
import com.spredfast.kafka.connect.s3.Layout;
import com.spredfast.kafka.connect.s3.Metrics;
import com.spredfast.kafka.connect.s3.S3;
import com.spredfast.kafka.connect.s3.S3RecordFormat;
import com.spredfast.kafka.connect.s3.S3RecordsWriter;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3SinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(S3SinkTask.class);

  private Map<String, String> config;

  private final Map<TopicPartition, PartitionWriter> partitions = new LinkedHashMap<>();

  private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

  private long GZIPChunkThreshold = 67108864;

  private long GZIPFileThreshold = -1;

  private long flushIntervalMs = -1;

  private long gracePeriodMs = -1;

  private S3Writer s3;

  private Optional<Converter> keyConverter;

  private Converter valueConverter;

  private S3RecordFormat recordFormat;

  private Metrics metrics;

  private Map<String, String> tags;

  @Override
  public String version() {
    return Constants.VERSION;
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    config = new HashMap<>(props);

    configGet("compressed_block_size")
        .map(Long::parseLong)
        .ifPresent(chunkThreshold -> this.GZIPChunkThreshold = chunkThreshold);

    configGet("compressed_file_size")
        .map(Long::parseLong)
        .ifPresent(gzipThreshold -> this.GZIPFileThreshold = gzipThreshold);

    configGet("flush.interval.ms")
        .map(Long::parseLong)
        .ifPresent(flushIntervalMs -> this.flushIntervalMs = flushIntervalMs);

    configGet("flush.grace.period.ms")
        .map(Long::parseLong)
        .ifPresentOrElse(
            gracePeriodMs -> this.gracePeriodMs = gracePeriodMs,
            () -> gracePeriodMs = flushIntervalMs > 0 ? flushIntervalMs / 2 : -1);

    recordFormat = Configure.createFormat(props);

    keyConverter = ofNullable(Configure.buildConverter(config, "key.converter", true, null));
    valueConverter =
        Configure.buildConverter(config, "value.converter", false, AlreadyBytesConverter.class);

    String bucket =
        configGet("s3.bucket")
            .filter(s -> !s.isEmpty())
            .orElseThrow(() -> new ConnectException("S3 bucket must be configured"));
    String prefix = configGet("s3.prefix").orElse("");
    AmazonS3 s3Client = S3.s3client(config);

    Layout layout = Configure.createLayout(props);

    s3 = new S3Writer(bucket, prefix, layout.getBuilder(), s3Client);

    metrics = Configure.metrics(props);
    tags = Configure.parseTags(props.get("metrics.tags"));
    tags.put("connector_name", name());

    // Recover initial assignments
    open(context.assignment());
  }

  private Optional<String> configGet(String key) {
    return ofNullable(config.get(key));
  }

  @Override
  public void stop() throws ConnectException {
    // ensure we delete our temp files
    for (PartitionWriter writer : partitions.values()) {
      log.debug("{} Stopping - Deleting temp file {}", name(), writer.getDataFile());
      writer.delete();
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

    // here we flush in the following cases:
    // * when no thresholds are defined (in this case flush interval is controlled by Kafka Connect
    // settings)
    // * when no new records were received for a long period of time
    partitions.values().stream()
        .filter(PartitionWriter::shouldFlush)
        .collect(toList())
        .forEach(PartitionWriter::done);

    log.debug("{} performing preCommit with offsets: {}", name(), offsetsToCommit);
    Map<TopicPartition, OffsetAndMetadata> result = offsetsToCommit;
    offsetsToCommit = new HashMap<>();
    return result;
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    records.stream()
        .collect(groupingBy(record -> new TopicPartition(record.topic(), record.kafkaPartition())))
        .forEach(
            (tp, rs) -> {
              long lastOffset = rs.get(rs.size() - 1).kafkaOffset();

              log.trace(
                  "{} received {} records for {} to archive. Last offset {}",
                  name(),
                  rs.size(),
                  tp,
                  lastOffset);

              for (SinkRecord r : rs) {
                PartitionWriter writer = partitions.computeIfAbsent(tp, t -> initWriter(t, r));

                // checking if timestamps based flushing should be done
                if (writer.shouldFlushBefore(r)) {
                  writer.done();
                  writer = partitions.computeIfAbsent(tp, t -> initWriter(t, r));
                }

                writer.writeRecord(r);

                // checking if file size based flushing should be done
                if (writer.shouldFlushAfter()) {
                  writer.done();
                }
              }
            });
  }

  private String name() {
    return configGet("name")
        .orElseThrow(() -> new IllegalWorkerStateException("Tasks always have names"));
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    // have already flushed, so just ensure the temp files are deleted (in case flush threw an
    // exception)
    partitions.stream()
        .map(this.partitions::get)
        .filter(p -> p != null)
        .forEach(PartitionWriter::delete);
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    // nothing to do. we will create files when we are given the first record for a partition
    // offsets are managed by Connect
  }

  private PartitionWriter initWriter(TopicPartition tp, SinkRecord firstRecord) {
    try {
      return new PartitionWriter(tp, firstRecord);
    } catch (IOException e) {
      throw new RetriableException(
          "Error initializing writer for " + tp + " at offset " + firstRecord.kafkaOffset(), e);
    }
  }

  private class PartitionWriter {
    private final TopicPartition tp;
    private final BlockGZIPFileWriter writer;
    private final S3RecordsWriter format;
    private final Map<String, String> tags;
    private boolean finished;
    private boolean closed;
    private final SinkRecord firstRecord;
    private SinkRecord lastRecord;
    private long lastRecordReceiveTime;

    private PartitionWriter(TopicPartition tp, SinkRecord firstRecord) throws IOException {
      this.tp = tp;
      this.firstRecord = firstRecord;
      format = recordFormat.newWriter();

      String localBufferDirectory =
          configGet("local.buffer.dir")
              .orElseThrow(() -> new ConnectException("No local buffer directory configured"));

      File directory = new File(localBufferDirectory);
      if (!directory.exists() && !directory.mkdirs()) {
        throw new ConnectException("Could not create directory " + localBufferDirectory);
      }

      Map<String, String> writerTags = new HashMap<>(S3SinkTask.this.tags);
      writerTags.put("kafka_topic", tp.topic());
      writerTags.put("kafka_partition", "" + tp.partition());
      this.tags = writerTags;

      writer =
          new BlockGZIPFileWriter(
              directory,
              firstRecord.kafkaOffset(),
              GZIPChunkThreshold,
              format.init(tp.topic(), tp.partition(), firstRecord.kafkaOffset()));
    }

    public BlockGZIPFileWriter getWriter() {
      return writer;
    }

    /**
     * The method checks whether a periodic flushing should be done.
     *
     * <p>If the flushing interval and the file size thresholds are not set it always returns true
     * letting Kafka Connect to control when to flush.
     *
     * <p>Otherwise, if the flushing interval is defined and there were no new records to cause file
     * size or timestamps based flushing it permits flushing.
     *
     * <p>Specifically it permits flushing when the time since the first record produced exceeds
     * flushIntervalMs + gracePeriodMs (this guarantees any record produced at this point in time
     * also satisfies timestamps based flushing criteria) and the time since the last record
     * received exceeds gracePeriodMs (this prioritizes files size and/or timestamps based flushing
     * when processing lagging messages).
     *
     * @return boolean whether to flush the partition writer
     */
    public boolean shouldFlush() {
      // by default, we skip smart checks and this way fallback to time based flushes
      // with timeouts configured on Kafka Connect worker level
      if (flushIntervalMs == -1 && GZIPFileThreshold == -1) {
        return true;
      }

      long now = Instant.now().toEpochMilli();
      long timeSinceFirstRecordProduced = now - firstRecord.timestamp();
      long timeSinceLastRecordReceived = now - lastRecordReceiveTime;

      boolean doWallTimeFlush =
          flushIntervalMs != -1
              && timeSinceFirstRecordProduced >= (flushIntervalMs + gracePeriodMs)
              && timeSinceLastRecordReceived > gracePeriodMs;
      if (doWallTimeFlush) {
        log.debug("{} performing a wall time flush on {}", name(), tp);
        return true;
      }

      return false;
    }

    /**
     * The method checks whether the record r exceeds flushIntervalMs and therefore the partition
     * writer must be flushed before accepting this record.
     *
     * @param r SinkRecord the next record to be written to the partition writer.
     * @return boolean whether to flush the partition writer
     */
    public boolean shouldFlushBefore(SinkRecord r) {
      long timeSinceFirstRecord = r.timestamp() - firstRecord.timestamp();
      boolean doPeriodicFlush = flushIntervalMs != -1 && timeSinceFirstRecord >= flushIntervalMs;
      if (doPeriodicFlush) {
        log.debug("{} performing a timestamp flush on {}", name(), tp);
        return true;
      }

      return false;
    }

    /**
     * The method checks whether the lastly written record caused the file size to exceed
     * GZIPFileThreshold. If it exceeded then the partition writer must be flushed before accepting
     * any new records.
     *
     * @return boolean whether to flush the partition writer
     */
    public boolean shouldFlushAfter() {
      boolean doFileSizeFlush =
          GZIPFileThreshold != -1 && writer.getTotalCompressedSize() > GZIPFileThreshold;
      if (doFileSizeFlush) {
        log.debug("{} performing a file size flush on {}", name(), tp);
        return true;
      }

      return false;
    }

    private void writeRecord(SinkRecord r) {
      try (Metrics.StopTimer ignored = metrics.time("writeRecord", tags)) {
        ProducerRecord<byte[], byte[]> pr =
            new ProducerRecord<>(
                r.topic(),
                r.kafkaPartition(),
                keyConverter
                    .map(c -> c.fromConnectData(r.topic(), r.keySchema(), r.key()))
                    .orElse(null),
                valueConverter.fromConnectData(r.topic(), r.valueSchema(), r.value()));

        byte[] formatted = format.write(pr);

        writer.write(formatted, 1);
      } catch (IOException e) {
        throw new RetriableException("Failed to write to buffer", e);
      }

      lastRecord = r;
      lastRecordReceiveTime = Instant.now().toEpochMilli();
    }

    public File getDataFile() {
      return writer.getDataFile();
    }

    public void delete() {
      writer.delete();
      partitions.remove(tp);
    }

    public void done() {
      Metrics.StopTimer time = metrics.time("s3Put", tags);
      try {
        if (!finished) {
          writer.write(format.finish(tp.topic(), tp.partition()), 0);
          finished = true;
        }
        if (!closed) {
          writer.close();
          closed = true;
        }
        final BlockMetadata blockMetadata = new BlockMetadata(tp, writer.getStartOffset());
        s3.putChunk(writer.getDataFile(), writer.getIndexFile(), blockMetadata);
      } catch (IOException e) {
        throw new RetriableException("Error flushing " + tp, e);
      }

      // here + 1 is required as the committed offset must point the first unprocessed message
      offsetsToCommit.put(tp, new OffsetAndMetadata(lastRecord.kafkaOffset() + 1));
      log.debug(
          "{} updating safe to commit offsets with pair {}:{}",
          name(),
          tp,
          offsetsToCommit.get(tp));

      delete();
      time.stop();
    }
  }
}
