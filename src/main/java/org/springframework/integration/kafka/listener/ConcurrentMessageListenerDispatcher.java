/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.springframework.integration.kafka.listener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.impl.factory.Maps;

import org.springframework.context.Lifecycle;
import org.springframework.integration.kafka.core.KafkaMessage;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.Assert;

/**
 * Dispatches {@link KafkaMessage}s across a set of MessageListeners.
 *
 * @author Marius Bogoevici
 */
public class ConcurrentMessageListenerDispatcher implements Lifecycle {

	public static final CustomizableThreadFactory THREAD_FACTORY = new CustomizableThreadFactory("dispatcher-");

	private static final StartDelegateProcedure startDelegateProcedure = new StartDelegateProcedure();

	private static final StopDelegateProcedure stopDelegateProcedure = new StopDelegateProcedure();

	private final Object lifecycleMonitor = new Object();

	private final Collection<Partition> partitions;

	private final int consumers;

	private volatile boolean running;

	private MessageListener delegateListener;

	private OffsetManager offsetManager;

	private MutableMap<Partition, QueueingMessageListenerInvoker> delegates;

	private int queueSize = 1024;

	private Executor taskExecutor;

	private ErrorHandler errorHandler = new LoggingErrorHandler();

	public ConcurrentMessageListenerDispatcher(MessageListener delegateListener, Collection<Partition> partitions, int consumers, OffsetManager offsetManager) {
		Assert.notEmpty(partitions, "A set of partitions must be provided");
		Assert.isTrue(consumers <= partitions.size(), "The number of consumers must be smaller or equal to the number of partitions");
		Assert.notNull(delegateListener, "A delegate must be provided");
		this.delegateListener = delegateListener;
		this.partitions = partitions;
		this.consumers = consumers;
		this.offsetManager = offsetManager;
	}

	public ErrorHandler getErrorHandler() {
		return errorHandler;
	}

	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	public OffsetManager getOffsetManager() {
		return offsetManager;
	}

	public void setOffsetManager(OffsetManager offsetManager) {
		this.offsetManager = offsetManager;
	}

	public int getQueueSize() {
		return queueSize;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	@Override
	public void start() {
		synchronized (lifecycleMonitor) {
			if (!isRunning()) {
				initializeAndStartDispatching();
				this.running = true;
			}
		}
	}

	@Override
	public void stop() {
		synchronized (lifecycleMonitor) {
			if (isRunning()) {
				this.running = false;
				delegates.flip().keyBag().toSet().forEach(stopDelegateProcedure);
			}
		}
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	public void dispatch(KafkaMessage message) {
		delegates.get(message.getMetadata().getPartition()).enqueue(message);
	}

	private void initializeAndStartDispatching() {
		// allocate delegate instances index them
		List<QueueingMessageListenerInvoker> delegateList = new ArrayList<QueueingMessageListenerInvoker>(consumers);
		for (int i = 0; i < consumers; i++) {
			QueueingMessageListenerInvoker blockingQueueMessageListenerInvoker = new QueueingMessageListenerInvoker(queueSize, offsetManager, delegateListener);
			if (errorHandler != null) {
				blockingQueueMessageListenerInvoker.setErrorHandler(errorHandler);
			}
			delegateList.add(blockingQueueMessageListenerInvoker);
		}
		// evenly distribute partitions across delegates
		delegates = Maps.mutable.of();
		int i = 0;
		for (Partition partition : partitions) {
			delegates.put(partition, delegateList.get((i++) % consumers));
		}
		// initialize task executor
		if (this.taskExecutor == null) {
			this.taskExecutor = Executors.newFixedThreadPool(consumers, THREAD_FACTORY);
		}
		// start dispatchers
		delegates.flip().keyBag().toSet().forEachWith(startDelegateProcedure, taskExecutor);
	}

	@SuppressWarnings("serial")
	private static class StopDelegateProcedure implements Procedure<QueueingMessageListenerInvoker> {
		@Override
		public void value(QueueingMessageListenerInvoker delegate) {
			delegate.stop();
		}
	}

	@SuppressWarnings("serial")
	private static class StartDelegateProcedure implements Procedure2<QueueingMessageListenerInvoker, Executor> {
		@Override
		public void value(QueueingMessageListenerInvoker delegate, Executor executor) {
			delegate.start();
			executor.execute(delegate);
		}
	}
}
