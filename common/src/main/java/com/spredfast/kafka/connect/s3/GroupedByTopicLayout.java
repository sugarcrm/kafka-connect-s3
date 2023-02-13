package com.spredfast.kafka.connect.s3;

import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.TopicPartition;

public class GroupedByTopicLayout implements Layout {

  private final Supplier<String> dateSupplier;

  public GroupedByTopicLayout(Supplier<String> dateSupplier) {
    this.dateSupplier = dateSupplier;
  }

  public Layout.Builder getBuilder() {
    return new Builder(dateSupplier);
  }

  public Layout.Parser getParser() {
    return new Parser();
  }

  static class Builder implements Layout.Builder {

    private final Supplier<String> dateSupplier;

    public Builder(Supplier<String> dateSupplier) {
      this.dateSupplier = dateSupplier;
    }

    @Override
    public String buildBlockPath(BlockMetadata blockMetadata) {
      final TopicPartition tp = blockMetadata.getTopicPartition();
      return String.format(
          "%s/%s/%05d-%012d",
          tp.topic(), dateSupplier.get(), tp.partition(), blockMetadata.getStartOffset());
    }

    @Override
    public String buildIndexPath(TopicPartition topicPartition) {
      return String.format(
          "%s/last_chunk_index.%05d.txt", topicPartition.topic(), topicPartition.partition());
    }
  }

  public static class Parser implements Layout.Parser {

    private static final Pattern KEY_PATTERN =
        Pattern.compile(
            // match the / or the start of the key, so we shouldn't have to worry about the prefix
            "(/|^)"
                // assuming no / in topic names
                + "(?<topic>[^/]+?)/"
                + "(?<date>[^/]+?)/"
                + "(?<partition>\\d{5})-"
                + "(?<offset>\\d{12})\\.gz$");

    @Override
    public BlockMetadata parseBlockPath(String path) {
      final Matcher matcher = KEY_PATTERN.matcher(path);
      if (!matcher.find()) {
        throw new IllegalArgumentException("Invalid block path: " + path);
      }
      final String topic = matcher.group("topic");
      final int partition = Integer.parseInt(matcher.group("partition"));
      final long startOffset = Long.parseLong(matcher.group("offset"));

      return new BlockMetadata(new TopicPartition(topic, partition), startOffset);
    }
  }
}
