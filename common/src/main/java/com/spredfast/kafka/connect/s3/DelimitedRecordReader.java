package com.spredfast.kafka.connect.s3;

import static java.util.Optional.ofNullable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Reads records that are followed by byte delimiters.
 */
public class DelimitedRecordReader implements RecordReader {
	private final byte[] valueDelimiter;

	private final Optional<byte[]> keyDelimiter;

	private final static int DEFAULT_BUFFER_SIZE = 32 * 1024 * 1024;

	private byte[] buf1;
	private byte[] buf2;
	private int bufLen = 0;
	private int bufPos = 0;
	private InputStream in;

	public DelimitedRecordReader(byte[] valueDelimiter, Optional<byte[]> keyDelimiter) {
		this(valueDelimiter, keyDelimiter, DEFAULT_BUFFER_SIZE);
	}

	public DelimitedRecordReader(byte[] valueDelimiter, Optional<byte[]> keyDelimiter, int bufferSize) {
		this.valueDelimiter = valueDelimiter;
		this.keyDelimiter = keyDelimiter;
		buf1 = new byte[bufferSize];
		buf2 = new byte[bufferSize];
	}

	@Override
	public ConsumerRecord<byte[], byte[]> read(String topic, int partition, long offset, BufferedInputStream data) throws IOException {
		if (in == null) {
			in = data;
		} else if (in != data) {
			throw new RuntimeException("Input stream object has changed");
		}

		Optional<byte[]> key = Optional.empty();
		if (keyDelimiter.isPresent()) {
			key = Optional.ofNullable(readTo(data, keyDelimiter.get()));
			if (!key.isPresent()) {
				return null;
			}
		}
		byte[] value = readTo(data, valueDelimiter);
		if (value == null) {
			if(key.isPresent()) {
				throw new IllegalStateException("missing value for key!" + new String(key.get()));
			}
			return null;
		}
		return new ConsumerRecord<>(
			topic, partition, offset, key.orElse(null), value
		);
	}

	private byte[] readTo(InputStream in, byte[] del) throws IOException {
		int delPos = indexOf(buf1, del, bufPos, bufLen);

		if (delPos == -1) {
			System.arraycopy(buf1, bufPos, buf2, 0, bufLen - bufPos);
			bufLen = bufLen - bufPos;
			bufPos = 0;

			while (bufLen < buf2.length) {
				int read = in.read(buf2, bufLen, buf2.length - bufLen);
				if (read == -1) {
					if (bufLen == 0) {
						return null;
					}
					break;
				}
				bufLen += read;
			}

			byte[] tmp = buf2;
			buf2 = buf1;
			buf1 = tmp;

			delPos = indexOf(buf1, del, bufPos, bufLen);
		}

		if (delPos == -1) {
			if (buf1.length == bufLen) {
				throw new RuntimeException("Couldn't find the delimiter although the buffer is full. " +
					"This might mean that a single message doesn't fit to the buffer and you need to increase the buffer size");
			} else {
				throw new RuntimeException("Couldn't find the delimiter while the buffer is not filled completely from the input stream");
			}
		}

		byte[] result = Arrays.copyOfRange(buf1, bufPos, delPos);
		bufPos = delPos + del.length;
		return result;
	}

	private int indexOf(byte[] buff, byte[] del, int from, int to) {
		outer:
		for (int i = from; i <= to - del.length; i++) {
			for (int j = 0; j < del.length; j++) {
				if (buff[i + j] != del[j]) {
					continue outer;
				}
			}
			return i;
		}
		return -1;
	}

	private static byte[] delimiterBytes(String value, String encoding) throws UnsupportedEncodingException {
		return ofNullable(value).orElse(TrailingDelimiterFormat.DEFAULT_DELIMITER).getBytes(
			ofNullable(encoding).map(Charset::forName).orElse(TrailingDelimiterFormat.DEFAULT_ENCODING)
		);
	}

	public static RecordReader from(Map<String, String> taskConfig) throws UnsupportedEncodingException {
		return new DelimitedRecordReader(
			delimiterBytes(taskConfig.get("value.converter.delimiter"), taskConfig.get("value.converter.encoding")),
			taskConfig.containsKey("key.converter")
				? Optional.of(delimiterBytes(taskConfig.get("key.converter.delimiter"), taskConfig.get("key.converter.encoding")))
				: Optional.empty()
		);
	}
}
