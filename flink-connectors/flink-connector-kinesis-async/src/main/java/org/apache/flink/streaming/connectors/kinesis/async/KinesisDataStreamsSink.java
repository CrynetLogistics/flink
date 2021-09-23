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
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * A Kinesis Data Streams (KDS) Sink that performs async requests against a destination stream using the
 * buffering protocol specified in {@link AsyncSinkBase}.
 *
 * <p>The sink internally uses a {@link software.amazon.awssdk.services.kinesis.KinesisAsyncClient}
 * to communicate with the AWS endpoint.
 *
 * <p>The behaviour of the buffering may be specified by providing configuration during the sink
 * build time.
 *
 * <ul>
 *   <li>{@code maxBatchSize}: the maximum size of a batch of entries that may be sent to KDS
 *   <li>{@code maxInFlightRequests}: the maximum number of in flight requests that may exist, if
 *   any more in flight requests need to be initiated once the maximum has been reached, then it
 *   will be blocked until some have completed
 *   <li>{@code maxBufferedRequests}: the maximum number of elements held in the buffer, requests to
 *   add elements will be blocked while the number of elements in the buffer is at the maximum
 *   <li>{@code flushOnBufferSizeInBytes}: if the total size in bytes of all elements in the buffer
 *   reaches this value, then a flush will occur the next time any elements are added to the buffer
 *   <li>{@code maxTimeInBufferMS}: the maximum amount of time an entry is allowed to live in the
 *   buffer, if any element reaches this age, the entire buffer will be flushed immediately
 * </ul>
 *
 * <p>Please see the writer implementation in {@link KinesisDataStreamsSinkWriter}
 *
 * @param <InputT> Type of the elements handled by this sink
 */
@PublicEvolving
public class KinesisDataStreamsSink<InputT> extends AsyncSinkBase<InputT, PutRecordsRequestEntry> {

    private final ElementConverter<InputT, PutRecordsRequestEntry> elementConverter;
    private final int maxBatchSize;
    private final int maxInFlightRequests;
    private final int maxBufferedRequests;
    private final long flushOnBufferSizeInBytes;
    private final long maxTimeInBufferMS;

    KinesisDataStreamsSink(
            ElementConverter<InputT, PutRecordsRequestEntry> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long flushOnBufferSizeInBytes,
            long maxTimeInBufferMS) {
        this.elementConverter = elementConverter;
        this.maxBatchSize = maxBatchSize;
        this.maxInFlightRequests = maxInFlightRequests;
        this.maxBufferedRequests = maxBufferedRequests;
        this.flushOnBufferSizeInBytes = flushOnBufferSizeInBytes;
        this.maxTimeInBufferMS = maxTimeInBufferMS;
    }

    /**
     * Create a {@link Builder} to construct a new {@link
     * KinesisDataStreamsSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link Builder}
     */
    public static <InputT> Builder<InputT> builder() {
        return new Builder<>();
    }

    @Override
    public SinkWriter<InputT, Void, Collection<PutRecordsRequestEntry>> createWriter(
            InitContext context, List<Collection<PutRecordsRequestEntry>> states) {
        return new KinesisDataStreamsSinkWriter<>(elementConverter, context, maxBatchSize,
                maxInFlightRequests, maxBufferedRequests, flushOnBufferSizeInBytes,
                maxTimeInBufferMS);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<PutRecordsRequestEntry>>>
            getWriterStateSerializer() {
        return Optional.empty();
    }

    //todo: sanity check variables! at both set() time and build() time
    public static class Builder<InputT> {

        private ElementConverter<InputT, PutRecordsRequestEntry> elementConverter;
        private int maxBatchSize;
        private int maxInFlightRequests;
        private int maxBufferedRequests;
        private long flushOnBufferSizeInBytes;
        private long maxTimeInBufferMS;

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

        public Builder<InputT> setFlushOnBufferSizeInBytes(
                long flushOnBufferSizeInBytes) {
            this.flushOnBufferSizeInBytes = flushOnBufferSizeInBytes;
            return this;
        }

        public Builder<InputT> setMaxTimeInBufferMS(long maxTimeInBufferMS) {
            this.maxTimeInBufferMS = maxTimeInBufferMS;
            return this;
        }

        public KinesisDataStreamsSink<InputT> build() {
            return new KinesisDataStreamsSink<>(
                    elementConverter,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    flushOnBufferSizeInBytes,
                    maxTimeInBufferMS);
        }
    }
}
