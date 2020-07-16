package com.spredfast.kafka.connect.s3;

import com.spredfast.kafka.connect.s3.source.*;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class S3SourceTaskTest {

	@Test
	public void testGetTopicMappingConfig() {

		Map<String, String> config = new HashMap<String, String>();
		config.put(S3SourceTask.CONFIG_TARGET_TOPIC + "." + "*.\\.stack.data", "ddd.stack.data");
		config.put(S3SourceTask.CONFIG_TARGET_TOPIC + "." + "^bbb$", "ddd.stack.data");
		config.put("eee", "eee");

		Map<String, String> topicMappingConfig = S3SourceTask.getTopicMappingConfig(config);

		assertEquals(topicMappingConfig.get("*.\\.stack.data"), "ddd.stack.data");
		assertEquals(topicMappingConfig.get("^bbb$"), "ddd.stack.data");
		assertNull(topicMappingConfig.get("ccc.stack.data"));
		assertEquals(topicMappingConfig.getOrDefault("eee", ""), "");
	}

//	@Test
//	public void testRemapTopic() {
//
//		S3SourceTask task = new S3SourceTask();
//
//		Map<String, String> config = new HashMap<String, String>();
//		config.put(S3SourceTask.CONFIG_TARGET_TOPIC + "." + ".*\\.stack\\.data", "new.stack.data");
//		config.put(S3SourceTask.CONFIG_TARGET_TOPIC + "." + "^bbb$", "new.stack.data");
//		config.put(S3SourceTask.CONFIG_TARGET_TOPIC_IGNORE_MISSING, "true");
//		task.init(config);
//
//		assertEquals(task.remapTopic("aaa.stack.data"), "new.stack.data");
//		assertEquals(task.remapTopic("qqq"), "");
//		assertEquals(task.remapTopic("bbb"), "new.stack.data");
//		assertEquals(task.remapTopic("bb"), "");
//
//		task = new S3SourceTask();
//		config.put(S3SourceTask.CONFIG_TARGET_TOPIC_IGNORE_MISSING, "false");
//		task.init(config);
//
//		assertEquals(task.remapTopic("ccc.stack.data"), "new.stack.data");
//		assertEquals(task.remapTopic("qqq"), "qqq");
//		assertEquals(task.remapTopic("bbb"), "new.stack.data");
//		assertEquals(task.remapTopic("bb"), "bb");
//	}
}
