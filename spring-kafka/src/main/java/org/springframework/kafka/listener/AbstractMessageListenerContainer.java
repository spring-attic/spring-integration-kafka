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
package org.springframework.kafka.listener;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.util.Assert;

/**
 *
 * @author Gary Russell
 */
public abstract class AbstractMessageListenerContainer implements BeanNameAware {

	private String beanName;

	private Object messageListener;

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	public String getBeanName() {
		return this.beanName;
	}

	/**
	 * Set the message listener; must be a {@link MessageListener} or
	 * {@link ConsumerAwareMessageListener}.
	 * @param messageListener the listener.
	 */
	public void setMessageListener(Object messageListener) {
		Assert.isTrue(
				messageListener instanceof MessageListener || messageListener instanceof ConsumerAwareMessageListener,
				"Either a " + MessageListener.class.getName() + " or a " + ConsumerAwareMessageListener.class.getName()
						+ " must be provided");
		this.messageListener = messageListener;
	}

	public Object getMessageListener() {
		return messageListener;
	}

}
