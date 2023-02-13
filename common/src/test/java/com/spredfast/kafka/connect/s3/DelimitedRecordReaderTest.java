package com.spredfast.kafka.connect.s3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

public class DelimitedRecordReaderTest {
  @Test(expected = RuntimeException.class)
  public void testNoDelimiterAtTheEnd() throws IOException {
    DelimitedRecordReader r =
        new DelimitedRecordReader("\n\n".getBytes(), Optional.of("\t\t".getBytes()), 10);

    BufferedInputStream in = inputStreamFor("key1\t\tvalue1\n\nkey2\t\tvalue2");
    ConsumerRecord<byte[], byte[]> record = r.read("t1", 0, 0, in);
    assertArrayEquals("key1".getBytes(), record.key());
    assertArrayEquals("value1".getBytes(), record.value());

    r.read("t1", 0, 0, in);
  }

  @Test(expected = RuntimeException.class)
  public void testSmallBuffer() throws IOException {
    DelimitedRecordReader r =
        new DelimitedRecordReader("\n\n".getBytes(), Optional.of("\t\t".getBytes()), 3);

    BufferedInputStream in = inputStreamFor("key1\t\tvalue1\n\nkey2\t\tvalue2");
    r.read("t1", 0, 0, in);
  }

  @Test
  public void testHappyPath() throws IOException {
    DelimitedRecordReader r =
        new DelimitedRecordReader("\n\n".getBytes(), Optional.of("\t\t".getBytes()), 10);

    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= 10; i++) {
      sb.append("key");
      sb.append(i);
      sb.append("\t\t");
      sb.append("value");
      sb.append(i);
      sb.append("\n\n");
    }

    BufferedInputStream in = inputStreamFor(sb.toString());

    for (int i = 1; i <= 10; i++) {
      ConsumerRecord<byte[], byte[]> record = r.read("t1", 0, 0, in);
      assertArrayEquals(("key" + i).getBytes(), record.key());
      assertArrayEquals(("value" + i).getBytes(), record.value());
    }

    assertNull(r.read("t1", 0, 0, in));
  }

  private BufferedInputStream inputStreamFor(String data) {
    return new BufferedInputStream(new ByteArrayInputStream(data.getBytes()));
  }
}
