/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.kafka.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.util.ObjectUtils;

/**
 * {@link ProducerListener} that logs exceptions thrown when sending messages.
 *
 * @author Marius Bogoevici
 * @since 1.3
 */
public class LoggingProducerListener extends DefaultProducerListener {

	private static final Log log = LogFactory.getLog(LoggingProducerListener.class);

	private boolean includeContents = true;

	private int maxContentLogged = 100;

	/**
	 * Whether the log message should include the contents (key and payload).
	 *
	 * @param includeContents true if the contents of the message should be logged
	 */
	public void setIncludeContents(boolean includeContents) {
		this.includeContents = includeContents;
	}

	/**
	 * The maximum amount of data to be logged for either key or password. As message sizes may vary and
	 * become fairly large, this allows limiting the amount of data sent to logs.
	 *
	 * @param maxContentLogged the maximum amount of data being logged.
	 */
	public void setMaxContentLogged(int maxContentLogged) {
		this.maxContentLogged = maxContentLogged;
	}

	@Override
	public void onError(String topic, Integer partition, Object key, Object payload, Exception exception) {
		// key and payload are intentionally left out, due to security and/or size concerns
		if (log.isErrorEnabled()) {
			StringBuffer logOutput = new StringBuffer();
			logOutput.append("Exception thrown when sending a message");
			if (includeContents) {
				logOutput.append(" with key='"
						+ toDisplayString(ObjectUtils.nullSafeToString(key), maxContentLogged) + "'");
				logOutput.append(" and payload='"
						+ toDisplayString(ObjectUtils.nullSafeToString(payload),maxContentLogged) + "'");
			}
			logOutput.append(" to topic " + topic);
			if (partition != null) {
				logOutput.append(" and partition " +partition);
			}
			logOutput.append(":");
			log.error(logOutput, exception);
		}
	}

	private String toDisplayString(String original, int maxCharacters) {
		if (original.length() <= maxCharacters) {
			return original;
		}
		return original.substring(0, maxCharacters) + "...";
	}
}
