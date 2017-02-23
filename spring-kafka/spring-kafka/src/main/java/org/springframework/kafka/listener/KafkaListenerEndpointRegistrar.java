/*
 * Copyright 2014-2015 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.util.Assert;

/**
 * Helper bean for registering {@link KafkaListenerEndpoint} with
 * a {@link KafkaListenerEndpointRegistry}.
 *
 * @author Stephane Nicoll
 * @author Juergen Hoeller
 * @author Artem Bilan
 * @since 1.4
 * @see org.springframework.kafka.annotation.KafkaListenerConfigurer
 */
public class KafkaListenerEndpointRegistrar implements BeanFactoryAware, InitializingBean {

	private final List<KafkaListenerEndpointDescriptor> endpointDescriptors =
			new ArrayList<KafkaListenerEndpointDescriptor>();

	private KafkaListenerEndpointRegistry endpointRegistry;

	private MessageHandlerMethodFactory messageHandlerMethodFactory;

	private KafkaListenerContainerFactory<?> containerFactory;

	private String containerFactoryBeanName;

	private BeanFactory beanFactory;

	private boolean startImmediately;

	/**
	 * Set the {@link KafkaListenerEndpointRegistry} instance to use.
	 * @param endpointRegistry the {@link KafkaListenerEndpointRegistry} instance to use.
	 */
	public void setEndpointRegistry(KafkaListenerEndpointRegistry endpointRegistry) {
		this.endpointRegistry = endpointRegistry;
	}

	/**
	 * @return the {@link KafkaListenerEndpointRegistry} instance for this
	 * registrar, may be {@code null}.
	 */
	public KafkaListenerEndpointRegistry getEndpointRegistry() {
		return this.endpointRegistry;
	}

	/**
	 * Set the {@link MessageHandlerMethodFactory} to use to configure the message
	 * listener responsible to serve an endpoint detected by this processor.
	 * <p>By default, {@link DefaultMessageHandlerMethodFactory} is used and it
	 * can be configured further to support additional method arguments
	 * or to customize conversion and validation support. See
	 * {@link DefaultMessageHandlerMethodFactory} javadoc for more details.
	 * @param kafkaHandlerMethodFactory the {@link MessageHandlerMethodFactory} instance.
	 */
	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory kafkaHandlerMethodFactory) {
		this.messageHandlerMethodFactory = kafkaHandlerMethodFactory;
	}

	/**
	 * @return the custom {@link MessageHandlerMethodFactory} to use, if any.
	 */
	public MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
		return this.messageHandlerMethodFactory;
	}

	/**
	 * Set the {@link KafkaListenerContainerFactory} to use in case a {@link KafkaListenerEndpoint}
	 * is registered with a {@code null} container factory.
	 * <p>Alternatively, the bean name of the {@link KafkaListenerContainerFactory} to use
	 * can be specified for a lazy lookup, see {@link #setContainerFactoryBeanName}.
	 * @param containerFactory the {@link KafkaListenerContainerFactory} instance.
	 */
	public void setContainerFactory(KafkaListenerContainerFactory<?> containerFactory) {
		this.containerFactory = containerFactory;
	}

	/**
	 * Set the bean name of the {@link KafkaListenerContainerFactory} to use in case
	 * a {@link KafkaListenerEndpoint} is registered with a {@code null} container factory.
	 * Alternatively, the container factory instance can be registered directly:
	 * see {@link #setContainerFactory(KafkaListenerContainerFactory)}.
	 * @param containerFactoryBeanName the {@link KafkaListenerContainerFactory} bean name.
	 * @see #setBeanFactory
	 */
	public void setContainerFactoryBeanName(String containerFactoryBeanName) {
		this.containerFactoryBeanName = containerFactoryBeanName;
	}

	/**
	 * A {@link BeanFactory} only needs to be available in conjunction with
	 * {@link #setContainerFactoryBeanName}.
	 * @param beanFactory the {@link BeanFactory} instance.
	 */
	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}


	@Override
	public void afterPropertiesSet() {
		registerAllEndpoints();
	}

	protected void registerAllEndpoints() {
		synchronized (this.endpointDescriptors) {
			for (KafkaListenerEndpointDescriptor descriptor : this.endpointDescriptors) {
				this.endpointRegistry.registerListenerContainer(
						descriptor.endpoint, resolveContainerFactory(descriptor));
			}
			this.startImmediately = true;  // trigger immediate startup
		}
	}

	private KafkaListenerContainerFactory<?> resolveContainerFactory(KafkaListenerEndpointDescriptor descriptor) {
		if (descriptor.containerFactory != null) {
			return descriptor.containerFactory;
		}
		else if (this.containerFactory != null) {
			return this.containerFactory;
		}
		else if (this.containerFactoryBeanName != null) {
			Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
			this.containerFactory = this.beanFactory.getBean(
					this.containerFactoryBeanName, KafkaListenerContainerFactory.class);
			return this.containerFactory;  // Consider changing this if live change of the factory is required
		}
		else {
			throw new IllegalStateException("Could not resolve the " +
					KafkaListenerContainerFactory.class.getSimpleName() + " to use for [" +
					descriptor.endpoint + "] no factory was given and no default is set.");
		}
	}

	/**
	 * Register a new {@link KafkaListenerEndpoint} alongside the
	 * {@link KafkaListenerContainerFactory} to use to create the underlying container.
	 * <p>The {@code factory} may be {@code null} if the default factory has to be
	 * used for that endpoint.
	 * @param endpoint the {@link KafkaListenerEndpoint} instance to register.
	 * @param factory the {@link KafkaListenerContainerFactory} to use.
	 */
	public void registerEndpoint(KafkaListenerEndpoint endpoint, KafkaListenerContainerFactory<?> factory) {
		Assert.notNull(endpoint, "Endpoint must be set");
		Assert.hasText(endpoint.getId(), "Endpoint id must be set");
		// Factory may be null, we defer the resolution right before actually creating the container
		KafkaListenerEndpointDescriptor descriptor = new KafkaListenerEndpointDescriptor(endpoint, factory);
		synchronized (this.endpointDescriptors) {
			if (this.startImmediately) { // Register and start immediately
				this.endpointRegistry.registerListenerContainer(descriptor.endpoint,
						resolveContainerFactory(descriptor), true);
			}
			else {
				this.endpointDescriptors.add(descriptor);
			}
		}
	}

	/**
	 * Register a new {@link KafkaListenerEndpoint} using the default
	 * {@link KafkaListenerContainerFactory} to create the underlying container.
	 * @param endpoint the {@link KafkaListenerEndpoint} instance to register.
	 * @see #setContainerFactory(KafkaListenerContainerFactory)
	 * @see #registerEndpoint(KafkaListenerEndpoint, KafkaListenerContainerFactory)
	 */
	public void registerEndpoint(KafkaListenerEndpoint endpoint) {
		registerEndpoint(endpoint, null);
	}


	private static class KafkaListenerEndpointDescriptor {

		private final KafkaListenerEndpoint endpoint;

		private final KafkaListenerContainerFactory<?> containerFactory;

		private KafkaListenerEndpointDescriptor(KafkaListenerEndpoint endpoint, KafkaListenerContainerFactory<?> containerFactory) {
			this.endpoint = endpoint;
			this.containerFactory = containerFactory;
		}

	}

}
