package com.spredfast.kafka.connect.s3.sink;

import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.kafka.common.TopicPartition;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.spredfast.kafka.connect.s3.json.ChunkDescriptor;
import com.spredfast.kafka.connect.s3.json.ChunksIndex;


/**
 * S3Writer provides necessary operations over S3 to store files and retrieve
 * Last commit offsets for a TopicPartition.
 * <p>
 * Maybe one day we could make this an interface and support pluggable storage backends...
 * but for now it's just to keep things simpler to test.
 */
public class S3Writer {
	private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
	private final ObjectReader reader = new ObjectMapper().readerFor(ChunksIndex.class);
	private String keyPrefix;
	private String bucket;
	private AmazonS3 s3Client;
	private TransferManager tm;

	public S3Writer(String bucket, String keyPrefix) {
		this(bucket, keyPrefix, new AmazonS3Client(new ProfileCredentialsProvider()));
	}

	public S3Writer(String bucket, String keyPrefix, AmazonS3 s3Client) {
		this(bucket, keyPrefix, s3Client, TransferManagerBuilder.standard().withS3Client(s3Client).build());
	}

	public S3Writer(String bucket, String keyPrefix, AmazonS3 s3Client, TransferManager tm) {
		if (keyPrefix.length() > 0 && !keyPrefix.endsWith("/")) {
			keyPrefix += "/";
		}
		this.keyPrefix = keyPrefix;
		this.bucket = bucket;
		this.s3Client = s3Client;
		this.tm = tm;
	}

	public void putChunk(File dataFile, File indexFile, TopicPartition tp, long firstRecordOffset) throws IOException {
		// Put data file then index, then finally update/create the last_index_file marker
		String dataObjectKey = this.getObjectKey(tp, firstRecordOffset, "gz");
		String indexObjectKey = this.getObjectKey(tp, firstRecordOffset, "index.json");

		try {
			Upload upload = tm.upload(this.bucket, dataObjectKey, dataFile);
			upload.waitForCompletion();
			upload = tm.upload(this.bucket, indexObjectKey, indexFile);
			upload.waitForCompletion();
		} catch (Exception e) {
			throw new IOException("Failed to upload to S3", e);
		}

		this.updateCursorFile(indexObjectKey, tp);
	}

	public long fetchOffset(TopicPartition tp) throws IOException {

		// See if cursor file exists
		String indexFileKey;

		try (
			S3Object cursorObj = s3Client.getObject(this.bucket, this.getTopicPartitionLastIndexFileKey(tp));
			InputStreamReader input = new InputStreamReader(cursorObj.getObjectContent(), "UTF-8");
		) {
			StringBuilder sb = new StringBuilder(1024);
			final char[] buffer = new char[1024];

			for (int read = input.read(buffer, 0, buffer.length);
				 read != -1;
				 read = input.read(buffer, 0, buffer.length)) {
				sb.append(buffer, 0, read);
			}
			indexFileKey = sb.toString();
		} catch (AmazonS3Exception ase) {
			if (ase.getStatusCode() == 404) {
				// Topic partition has no data in S3, start from beginning
				return 0;
			} else {
				throw new IOException("Failed to fetch cursor file", ase);
			}
		} catch (Exception e) {
			throw new IOException("Failed to fetch or read cursor file", e);
		}

		// Now fetch last written index file...
		try (
			S3Object indexObj = s3Client.getObject(this.bucket, indexFileKey);
			InputStreamReader isr = new InputStreamReader(indexObj.getObjectContent(), "UTF-8");
		) {
			return getNextOffsetFromIndexFileContents(isr);
		} catch (Exception e) {
			throw new IOException("Failed to fetch or parse last index file", e);
		}
	}

	private long getNextOffsetFromIndexFileContents(Reader indexJSON) throws IOException {
		ChunksIndex index = reader.readValue(indexJSON);
		ChunkDescriptor lastChunk = index.chunks.get(index.chunks.size() - 1);
		return lastChunk.first_record_offset + lastChunk.num_records;
	}

	// We store chunk files with a date prefix just to make finding them and navigating around the bucket a bit easier
	// date is meaningless other than "when this was uploaded"
	private String getObjectKey(TopicPartition tp, long firstRecordOffset, String extension) {
		final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		df.setTimeZone(UTC);
		final String date = df.format(new Date());

		return String.format("%s%s/%s/%05d-%012d.%s", keyPrefix, tp.topic(), date, tp.partition(), firstRecordOffset, extension);
	}

	private String getTopicPartitionLastIndexFileKey(TopicPartition tp) {
		return String.format("%s%s/last_chunk_index.%05d.txt", keyPrefix, tp.topic(), tp.partition());
	}

	private void updateCursorFile(String lastIndexFileKey, TopicPartition tp) throws IOException {
		try {
			byte[] contentAsBytes = lastIndexFileKey.getBytes("UTF-8");
			ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(contentAsBytes);
			ObjectMetadata md = new ObjectMetadata();
			md.setContentLength(contentAsBytes.length);
			s3Client.putObject(new PutObjectRequest(this.bucket, this.getTopicPartitionLastIndexFileKey(tp),
				contentsAsStream, md));
		} catch (Exception ex) {
			throw new IOException("Failed to update cursor file", ex);
		}
	}
}
