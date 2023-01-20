package com.spredfast.kafka.connect.s3.sink;

import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spredfast.kafka.connect.s3.json.ChunkDescriptor;
import com.spredfast.kafka.connect.s3.json.ChunksIndex;

/**
 * BlockGZIPFileWriter accumulates newline delimited UTF-8 records and writes them to an
 * output file that is readable by GZIP.
 * <p>
 * In fact this file is the concatenation of possibly many separate GZIP files corresponding to smaller chunks
 * of the input. Alongside the output filename.gz file, a file filename-index.json is written containing JSON
 * metadata about the size and location of each block.
 * <p>
 * This allows a reading class to skip to particular line/record without decompressing whole file by looking up
 * the offset of the containing block, seeking to it and beginning GZIp read from there.
 * <p>
 * This is especially useful when the file is an archive in HTTP storage like Amazon S3 where GET request with
 * range headers can allow pulling a small segment from overall compressed file.
 * <p>
 * Note that thanks to GZIP spec, the overall file is perfectly valid and will decompress as if it was a single stream
 * with any regular GZIP decoding library or program.
 */
public class BlockGZIPFileWriter implements Closeable {

	final private File dataFile;
	final private File indexFile;
	private GZIPOutputStream gzipStream;
	private CountingOutputStream fileStream;
	private final ObjectMapper objectMapper = new ObjectMapper();

	private class Chunk {
		public long rawBytes = 0;
		public long byteOffset = 0;
		public long compressedByteLength = 0;
		public long firstOffset = 0;
		public long numRecords = 0;

		ChunkDescriptor toJson() {
			ChunkDescriptor chunkObj = new ChunkDescriptor();
			chunkObj.first_record_offset = firstOffset;
			chunkObj.num_records = numRecords;
			chunkObj.byte_offset = byteOffset;
			chunkObj.byte_length = compressedByteLength;
			chunkObj.byte_length_uncompressed = rawBytes;
			return chunkObj;
		}
	}

	private class CountingOutputStream extends FilterOutputStream {
		private long numBytes = 0;

		CountingOutputStream(OutputStream out) throws IOException {
			super(out);
		}

		@Override
		public void write(int b) throws IOException {
			out.write(b);
			numBytes++;
		}

		@Override
		public void write(byte[] b) throws IOException {
			out.write(b);
			numBytes += b.length;
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			out.write(b, off, len);
			numBytes += len;
		}

		public long getNumBytesWritten() {
			return numBytes;
		}
	}

	private ArrayList<Chunk> chunks = new ArrayList<>();

	// Default each chunk is 64MB of uncompressed data
	private long chunkThreshold;

	// Start offset of the block.
	// Set to non-zero if this file is part of a larger stream and you want
	// record offsets in the index to reflect the global offset rather than local
	private long startOffset;

	public BlockGZIPFileWriter(File directory) throws IOException {
		this(directory, 0, 67108864);
	}

	public BlockGZIPFileWriter(File directory, long startOffset) throws IOException {
		this(directory, startOffset, 67108864);
	}

	public BlockGZIPFileWriter(File directory, long startOffset, long chunkThreshold) throws IOException {
		this(directory, startOffset, chunkThreshold, new byte[0]);
	}

	public BlockGZIPFileWriter(File directory, long startOffset, long chunkThreshold, byte[] header)
		throws IOException {
		this.startOffset = startOffset;
		this.chunkThreshold = chunkThreshold;

		// Initialize first chunk
		Chunk ch = new Chunk();
		ch.firstOffset = startOffset;
		chunks.add(ch);

		dataFile = File.createTempFile("data", null, directory);
		indexFile = File.createTempFile("index", null, directory);

		FileOutputStream fos = new FileOutputStream(dataFile);

		// Open file for writing and setup
		this.fileStream = new CountingOutputStream(fos);
		initChunkWriter();
		if (header.length > 0) {
			// if there is a header, write it as its own gzip chunk
			// so we know how many bytes to skip
			gzipStream.write(header);
			gzipStream.finish();
			gzipStream = new GZIPOutputStream(fileStream);
			// may have written header bytes
			ch.byteOffset = fileStream.getNumBytesWritten();
		}
	}

	private void initChunkWriter() throws IOException {
		gzipStream = new GZIPOutputStream(fileStream);
	}

	private Chunk currentChunk() {
		return chunks.get(chunks.size() - 1);
	}

	public File getDataFile() {
		return dataFile;
	}

	public File getIndexFile() {
		return indexFile;
	}

	public long getStartOffset() {
		return startOffset;
	}

	/**
	 *
	 * @param toWrite the bytes to write.
	 * @param recordCount how many records these bytes represent.
	 */
	public void write(List<byte[]> toWrite, long recordCount) throws IOException {
		Chunk ch = currentChunk();

		int rawBytesToWrite = 0;
		for (byte[] bytes : toWrite) {
			rawBytesToWrite += bytes.length;
		}

		if ((ch.rawBytes + rawBytesToWrite) > chunkThreshold) {
			finishChunk();
			initChunkWriter();

			Chunk newCh = new Chunk();
			newCh.firstOffset = ch.firstOffset + ch.numRecords;
			newCh.byteOffset = ch.byteOffset + ch.compressedByteLength;
			chunks.add(newCh);
			ch = newCh;
		}

		for (byte[] bytes : toWrite) {
			gzipStream.write(bytes);
		}

		ch.rawBytes += rawBytesToWrite;
		ch.numRecords += recordCount;
	}

	public void delete() {
		deleteIfExists(getDataFile());
		deleteIfExists(getIndexFile());
	}

	private void deleteIfExists(File file) {
		if (file.exists() && !file.isDirectory()) {
			//noinspection ResultOfMethodCallIgnored
			file.delete();
		}
	}

	private void finishChunk() throws IOException {
		Chunk ch = currentChunk();

		// Complete GZIP block without closing stream
		gzipStream.finish();

		// We can no find out how long this chunk was compressed
		long bytesWritten = fileStream.getNumBytesWritten();
		ch.compressedByteLength = bytesWritten - ch.byteOffset;
	}

	public void close() throws IOException {
		// Flush last chunk, updating index
		finishChunk();
		gzipStream.close();
		// Now close the writer (and the whole stream stack)
		writeIndex();
	}

	private void writeIndex() throws IOException {
		if (!indexFile.getParentFile().exists() && !indexFile.getParentFile().mkdirs()) {
			throw new IOException("Cannot create index " + indexFile);
		}

		objectMapper.writer().writeValue(indexFile, ChunksIndex.of(chunks.stream()
			.map(Chunk::toJson).collect(toList())));
	}

	public int getTotalUncompressedSize() {
		int totalBytes = 0;
		for (Chunk ch : chunks) {
			totalBytes += ch.rawBytes;
		}
		return totalBytes;
	}

	public int getNumChunks() {
		return chunks.size();
	}

	public int getNumRecords() {
		int totalRecords = 0;
		for (Chunk ch : chunks) {
			totalRecords += ch.numRecords;
		}
		return totalRecords;
	}
}
