package com.spredfast.kafka.connect.s3;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.util.Map;

public class S3 {

  public static AmazonS3 s3client(Map<String, String> config) {

    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

    String s3Region = config.get("s3.region");
    String s3Endpoint = config.get("s3.endpoint");

    // If worker config sets explicit endpoint override (e.g. for testing) use that
    if (s3Endpoint != null) {
      if (s3Region == null) {
        throw new IllegalArgumentException("s3.region must be set if s3.endpoint is set");
      }

      builder = builder.withEndpointConfiguration(new EndpointConfiguration(s3Endpoint, s3Region));
    } else if (s3Region != null) {
      builder = builder.withRegion(s3Region);
    }

    boolean s3PathStyle = Boolean.parseBoolean(config.get("s3.path_style"));

    return builder.withPathStyleAccessEnabled(s3PathStyle).build();
  }
}
