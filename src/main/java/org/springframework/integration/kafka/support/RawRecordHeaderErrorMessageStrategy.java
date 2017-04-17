/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.integration.kafka.support;

import java.util.Collections;
import java.util.Map;

import org.springframework.integration.handler.advice.ErrorMessageSendingRecoverer.DefaultRecovererErrorMessageStrategy;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.message.EnhancedErrorMessage;
import org.springframework.integration.support.ErrorMessagePublishingRecoveryCallback;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.retry.RetryContext;

/**
 * {@link DefaultRecovererErrorMessageStrategy} extension that adds the raw record as
 * a header to the {@link EnhancedErrorMessage}.
 *
 * @author Gary Russell
 * @since 2.1.1
 *
 */
public class RawRecordHeaderErrorMessageStrategy extends DefaultRecovererErrorMessageStrategy {

	@Override
	public ErrorMessage buildErrorMessage(RetryContext context) {
		Throwable lastThrowable = determinePayload(context);
		Object inputMessage = context
				.getAttribute(ErrorMessagePublishingRecoveryCallback.INPUT_MESSAGE_CONTEXT_KEY);
		Map<String, Object> headers = Collections.singletonMap(
				KafkaMessageDrivenChannelAdapter.RAW_RECORD,
				context.getAttribute(KafkaMessageDrivenChannelAdapter.RAW_RECORD));
		return inputMessage instanceof Message
				? new EnhancedErrorMessage((Message<?>) inputMessage, lastThrowable, headers)
				: new ErrorMessage(lastThrowable, headers);
	}

}
