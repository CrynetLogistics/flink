/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.async;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBaseConfig;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

/** Configuration for {@link KinesisDataStreamsSink}. */
@PublicEvolving
public class KinesisDataStreamsSinkConfig<InputT>
        extends AsyncSinkBaseConfig<InputT, PutRecordsRequestEntry> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 200;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 16;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10000;
    private static final long DEFAULT_FLUSH_ON_BUFFER_SIZE_IN_B = 64 * 1024 * 1024;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;

    private final String streamName;

    public KinesisDataStreamsSinkConfig(
            ElementConverter<InputT, PutRecordsRequestEntry> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long flushOnBufferSizeInBytes,
            long maxTimeInBufferMS,
            String streamName) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                flushOnBufferSizeInBytes,
                maxTimeInBufferMS);

        Preconditions.checkNotNull(
                streamName,
                "The stream name must be set and "
                        + "set to a non null value when initializing the KDS Sink.");
        this.streamName = streamName;
    }

    /**
     * Create a {@link KinesisDataStreamsSinkConfig.Builder} to allow the fluent and articulate
     * construction of a new {@link KinesisDataStreamsSinkConfig}.
     *
     * @param <InputT> type of incoming records
     * @return {@link KinesisDataStreamsSinkConfig.Builder}
     */
    public static <InputT> KinesisDataStreamsSinkConfig.Builder<InputT> builder() {
        return new KinesisDataStreamsSinkConfig.Builder<>();
    }

    public String getStreamName() {
        return streamName;
    }

    /** A builder for the encapsulating class, {@link KinesisDataStreamsSinkConfig}. */
    public static class Builder<InputT> {

        private ElementConverter<InputT, PutRecordsRequestEntry> elementConverter;
        private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
        private int maxInFlightRequests = DEFAULT_MAX_IN_FLIGHT_REQUESTS;
        private int maxBufferedRequests = DEFAULT_MAX_BUFFERED_REQUESTS;
        private long flushOnBufferSizeInBytes = DEFAULT_FLUSH_ON_BUFFER_SIZE_IN_B;
        private long maxTimeInBufferMS = DEFAULT_MAX_TIME_IN_BUFFER_MS;
        private String streamName;

        public Builder<InputT> setElementConverter(
                ElementConverter<InputT, PutRecordsRequestEntry> elementConverter) {
            this.elementConverter = elementConverter;
            return this;
        }

        public Builder<InputT> setMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder<InputT> setMaxInFlightRequests(int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        public Builder<InputT> setMaxBufferedRequests(int maxBufferedRequests) {
            this.maxBufferedRequests = maxBufferedRequests;
            return this;
        }

        public Builder<InputT> setFlushOnBufferSizeInBytes(long flushOnBufferSizeInBytes) {
            this.flushOnBufferSizeInBytes = flushOnBufferSizeInBytes;
            return this;
        }

        public Builder<InputT> setMaxTimeInBufferMS(long maxTimeInBufferMS) {
            this.maxTimeInBufferMS = maxTimeInBufferMS;
            return this;
        }

        public Builder<InputT> setStreamName(String streamName) {
            Preconditions.checkArgument(!streamName.isEmpty());
            this.streamName = streamName;
            return this;
        }

        public KinesisDataStreamsSinkConfig<InputT> build() {
            return new KinesisDataStreamsSinkConfig<>(
                    elementConverter,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    flushOnBufferSizeInBytes,
                    maxTimeInBufferMS,
                    streamName);
        }
    }
}
