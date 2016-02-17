/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.kafka.offset;

import java.util.Map;

import org.springframework.integration.kafka.core.DefaultConnectionFactory;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.core.ZookeeperConfiguration;
import org.springframework.integration.kafka.listener.KafkaNativeOffsetManager;
import org.springframework.integration.kafka.listener.OffsetManager;
import org.springframework.integration.kafka.listener.WindowingOffsetManager;
import org.springframework.integration.kafka.support.ZookeeperConnect;

/**
 * @author Artem Bilan
 * @since 1.3.1
 */
public class WindowingOffsetManagerTests extends AbstractOffsetManagerTests {

	@Override
	protected OffsetManager createOffsetManager(long referenceTimestamp,
	                                            String consumerId,
	                                            Map<Partition, Long> initialOffsets) throws Exception {
		ZookeeperConnect zookeeperConnect = new ZookeeperConnect(kafkaRule.getZookeeperConnectionString());
		KafkaNativeOffsetManager kafkaNativeOffsetManager =
				new KafkaNativeOffsetManager(new DefaultConnectionFactory(new ZookeeperConfiguration(zookeeperConnect)),
						zookeeperConnect,
						initialOffsets);
		kafkaNativeOffsetManager.setConsumerId(consumerId);
		kafkaNativeOffsetManager.afterPropertiesSet();
		kafkaNativeOffsetManager.setReferenceTimestamp(referenceTimestamp);

		WindowingOffsetManager windowingOffsetManager = new WindowingOffsetManager(kafkaNativeOffsetManager);
		windowingOffsetManager.setCount(2);
		windowingOffsetManager.setTimespan(10);
		windowingOffsetManager.afterPropertiesSet();
		return windowingOffsetManager;
	}
}
