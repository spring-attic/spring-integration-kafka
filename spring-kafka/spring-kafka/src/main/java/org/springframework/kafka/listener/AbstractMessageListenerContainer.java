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
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

/**
 *
 * @author Gary Russell
 */
public abstract class AbstractMessageListenerContainer<K, V> implements BeanNameAware, SmartLifecycle {

	public enum AckMode {
		RECORD, BATCH, TIME, COUNT, MANUAL
	}

	private final Object lifecycleMonitor = new Object();

	private String beanName;

	protected AckMode ackMode = AckMode.BATCH;

	private Object messageListener;

	protected volatile long pollTimeout = 1000;

	private boolean autoStartup = true;

	private int phase = 0;

	private volatile boolean running = false;


	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	public String getBeanName() {
		return this.beanName;
	}

	/**
	 * Set the message listener; must be a {@link MessageListener} or
	 * {@link AcknowledgingMessageListener}.
	 * @param messageListener the listener.
	 */
	public void setMessageListener(Object messageListener) {
		Assert.isTrue(
				messageListener instanceof MessageListener || messageListener instanceof AcknowledgingMessageListener,
				"Either a " + MessageListener.class.getName() + " or a " + AcknowledgingMessageListener.class.getName()
						+ " must be provided");
		this.messageListener = messageListener;
	}

	public Object getMessageListener() {
		return messageListener;
	}

	/**
	 * The ack mode to use when auto ack (in the configuration properties) is
	 * false.
	 * <ul>
	 * <li>RECORD: Ack after each record has been passed to the listener.</li>
	 * <li>BATCH: Ack after each batch of records received from the consumer has
	 * been passed to the listener</li>
	 * <li>TIME: Ack after this number of milliseconds;
	 * (should be greater than {@code #setPollTimeout(long) pollTimeout}.</li>
	 * <li>COUNT: Ack after at least this number of records have been received</li>
	 * <li>MANUAL: Listener is responsible for acking - use a
	 * {@link AcknowledgingMessageListener}.
	 * </ul>
	 * @param ackMode the {@link AckMode}; default BATCH.
	 */
	public void setAckMode(AckMode ackMode) {
		this.ackMode = ackMode;
	}

	/**
	 * @return the {@link AckMode}
	 * @see #setAckMode(AckMode)
	 */
	public AckMode getAckMode() {
		return ackMode;
	}

	/**
	 * The max time to block in the consumer waiting for records.
	 * @param pollTimeout the timeout in ms; default 1000.
	 */
	public void setPollTimeout(long pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	/**
	 * @return the poll timeout.
	 * @see #setPollTimeout(long)
	 */
	public long getPollTimeout() {
		return pollTimeout;
	}

	@Override
	public boolean isAutoStartup() {
		return autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public final void start() {
		synchronized (lifecycleMonitor) {
			doStart();
		}
	}

	protected abstract void doStart();

	@Override
	public final void stop() {
		stop(null);
	}

	@Override
	public void stop(Runnable callback) {
		synchronized (lifecycleMonitor) {
			doStop();
		}
		if (callback != null) {
			callback.run();
		}
	}

	protected abstract void doStop();

	protected void setRunning(boolean running) {
		this.running = running;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

}
