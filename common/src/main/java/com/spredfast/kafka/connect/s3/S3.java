package com.spredfast.kafka.connect.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.util.Map;

public class S3 {

	public static AmazonS3 s3client(Map<String, String> config) {
		boolean s3PathStyle = Boolean.parseBoolean(config.get("s3.path_style"));
		String s3Endpoint = config.get("s3.endpoint");

		AmazonS3 s3Client;
		if (s3Endpoint == null || s3Endpoint.equals("")) {
			s3Client = AmazonS3ClientBuilder
				.standard()
				.withPathStyleAccessEnabled(s3PathStyle)
				.build();
		} else {
			// Testing with localstack
			s3Client = AmazonS3ClientBuilder
				.standard()
				.withEndpointConfiguration(new EndpointConfiguration(config.get("s3.endpoint"), config.get("region")))
				.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(config.get("aws.accessKeyId"), config.get("aws.secretAccessKey"))))
				.withPathStyleAccessEnabled(s3PathStyle)
				.build();
		}

		return s3Client;
	}
}
