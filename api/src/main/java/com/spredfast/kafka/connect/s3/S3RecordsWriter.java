package com.spredfast.kafka.connect.s3;

import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerRecord;

/** */
public interface S3RecordsWriter {

  /** Opportunity to write any header bytes desired. */
  default byte[] init(String topic, int partition, long startOffset) {
    return new byte[0];
  }

  byte[] write(ProducerRecord<byte[], byte[]> record);

  /** Hook for writing any trailer bytes to the S3 file. */
  default byte[] finish(String topic, int partition) {
    return new byte[0];
  }

  static S3RecordsWriter forRecordWriter(
      Function<ProducerRecord<byte[], byte[]>, byte[]> writeRecord) {
    return (S3RecordsWriter) writeRecord;
  }
}
