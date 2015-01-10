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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.springframework.context.Lifecycle;
import org.springframework.integration.kafka.core.KafkaMessage;

/**
 * Invokes a delegate {@link MessageListener} for all the messages passed to it, storing them
 * in an internal queue.
 *
 * @author Marius Bogoevici
 */
public class QueueingMessageListenerInvoker implements Runnable, Lifecycle {

	private BlockingQueue<KafkaMessage> messages;

	private volatile boolean running = false;

	private MessageListener delegate;

	private OffsetManager offsetManager;

	private ErrorHandler errorHandler = new LoggingErrorHandler();

	public QueueingMessageListenerInvoker(int capacity, OffsetManager offsetManager, MessageListener delegate) {
		this.offsetManager = offsetManager;
		this.delegate = delegate;
		this.messages = new ArrayBlockingQueue<KafkaMessage>(capacity, true);
	}

	/**
	 * Adds a message to the queue, blocking if the queue has reached its maximum capacity.
	 *
	 * Interrupts will be ignored for as long as the component's {@code running} flag is set to true, but will
	 * be deferred for when the method returns.
	 *
	 * @param message
	 */
	public void enqueue(KafkaMessage message) {
		boolean wasInterruptedWhileRunning = false;
		if (this.running) {
			boolean added = false;
			// handle the case when the thread is interrupted while the adapter is still running
			// retry adding the message to the queue until either we succeed, or the adapter is stopped
			while (!added && this.running) {
				try {
					this.messages.put(message);
					added = true;
				}
				catch (InterruptedException e) {
					// we ignore the interruption signal if we are still running, but pass it on if we are stopped
					wasInterruptedWhileRunning = true;
				}
			}
		}
		if (wasInterruptedWhileRunning) {
			Thread.currentThread().interrupt();
		}
	}

	public ErrorHandler getErrorHandler() {
		return errorHandler;
	}

	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	@Override
	public void start() {
		this.running = true;
	}

	@Override
	public void stop() {
		this.running = false;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public void run() {
		while (this.running) {
			try {
				KafkaMessage message = messages.take();
				try {
					delegate.onMessage(message);
				}
				catch (Exception e) {
					errorHandler.handle(e, message);
				}
				finally {
					offsetManager.updateOffset(message.getMetadata().getPartition(), message.getMetadata().getNextOffset());
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}
}
