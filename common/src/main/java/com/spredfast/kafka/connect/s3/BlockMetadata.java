package com.spredfast.kafka.connect.s3;

import org.apache.kafka.common.TopicPartition;

final public class BlockMetadata {
    final private TopicPartition topicPartition;
    final private long startOffset;

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
