package com.spredfast.kafka.connect.s3;

import org.apache.kafka.common.TopicPartition;

public interface Layout {

    Builder getBuilder();

    Parser getParser();

    interface Builder {

        String buildBlockPath(BlockMetadata blockMetadata);

        String buildIndexPath(TopicPartition topicPartition);
    }

    interface Parser {
        BlockMetadata parseBlockPath(String path) throws IllegalArgumentException;
    }
}
