package com.spredfast.kafka.connect.s3;

import org.apache.kafka.common.TopicPartition;

public final class BlockMetadata {
  private final TopicPartition topicPartition;
  private final long startOffset;

  public BlockMetadata(TopicPartition topicPartition, long startOffset) {
    this.topicPartition = topicPartition;
    this.startOffset = startOffset;
  }

  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  public long getStartOffset() {
    return startOffset;
  }
}
