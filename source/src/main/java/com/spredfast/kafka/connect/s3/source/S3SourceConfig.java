package com.spredfast.kafka.connect.s3.source;

import java.util.List;

public class S3SourceConfig {
  public final String bucket;
  public String keyPrefix = "";
  public int pageSize = 500;
  public String startMarker = null; // for partial replay
  public S3FilesReader.InputFilter inputFilter = S3FilesReader.InputFilter.GUNZIP;
  public S3FilesReader.PartitionFilter partitionFilter = S3FilesReader.PartitionFilter.MATCH_ALL;
  public List<String> messageKeyExcludeList;

  public S3SourceConfig(String bucket) {
    this.bucket = bucket;
  }

  public S3SourceConfig(
      String bucket,
      String keyPrefix,
      int pageSize,
      String startMarker,
      S3FilesReader.InputFilter inputFilter,
      S3FilesReader.PartitionFilter partitionFilter,
      List<String> messageKeyExcludeList) {
    this.bucket = bucket;
    this.keyPrefix = keyPrefix;
    this.pageSize = pageSize;
    this.startMarker = startMarker;
    if (inputFilter != null) {
      this.inputFilter = inputFilter;
    }
    if (partitionFilter != null) {
      this.partitionFilter = partitionFilter;
    }
    this.messageKeyExcludeList = messageKeyExcludeList;
  }
}
