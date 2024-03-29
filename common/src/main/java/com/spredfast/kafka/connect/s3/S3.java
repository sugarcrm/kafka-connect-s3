package com.spredfast.kafka.connect.s3;

import java.net.URI;
import java.util.Map;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class S3 {

  public static S3Client s3client(Map<String, String> config) {

    S3ClientBuilder builder = S3Client.builder();

    String s3Region = config.get("s3.region");
    String s3Endpoint = config.get("s3.endpoint");

    // If worker config sets explicit endpoint override (e.g. for testing) use that
    if (s3Endpoint != null) {
      if (s3Region == null) {
        throw new IllegalArgumentException("s3.region must be set if s3.endpoint is set");
      }
      builder = builder.endpointOverride(URI.create(s3Endpoint));
    }

    if (s3Region != null) {
      builder = builder.region(Region.of(s3Region));
    }

    builder = builder.forcePathStyle(Boolean.parseBoolean(config.get("s3.path_style")));

    return builder.build();
  }
}
