/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.kafka.listener.adapter;

import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * A wrapper for either an {@link InvocableHandlerMethod} or
 * {@link DelegatingInvocableHandler}. All methods delegate to the
 * underlying handler.
 *
 * @author Gary Russell
 * @since 1.5
 *
 */
public class HandlerAdapter {

	private final InvocableHandlerMethod invokerHandlerMethod;

	private final DelegatingInvocableHandler delegatingHandler;

	public HandlerAdapter(InvocableHandlerMethod invokerHandlerMethod) {
		this.invokerHandlerMethod = invokerHandlerMethod;
		this.delegatingHandler = null;
	}

	public HandlerAdapter(DelegatingInvocableHandler delegatingHandler) {
		this.invokerHandlerMethod = null;
		this.delegatingHandler = delegatingHandler;
	}

	public Object invoke(Message<?> message, Object... providedArgs) throws Exception {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.invoke(message, providedArgs);
		}
		else {
			return this.delegatingHandler.invoke(message, providedArgs);
		}
	}

	public String getMethodAsString(Object payload) {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getMethod().toGenericString();
		}
		else {
			return this.delegatingHandler.getMethodNameFor(payload);
		}
	}

	public Object getBean() {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getBean();
		}
		else {
			return this.delegatingHandler.getBean();
		}
	}


}
