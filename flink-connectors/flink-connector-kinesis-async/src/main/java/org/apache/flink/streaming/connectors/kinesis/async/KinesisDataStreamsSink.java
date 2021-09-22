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

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** a. */
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
            long maxTimeInBufferMS,
            KinesisAsyncClient client) {
        this.elementConverter = elementConverter;
        this.maxBatchSize = maxBatchSize;
        this.maxInFlightRequests = maxInFlightRequests;
        this.maxBufferedRequests = maxBufferedRequests;
        this.flushOnBufferSizeInBytes = flushOnBufferSizeInBytes;
        this.maxTimeInBufferMS = maxTimeInBufferMS;
    }

    /**
     * Create a {@link KinesisDataStreamsSinkBuilder} to construct a new {@link
     * KinesisDataStreamsSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link KinesisDataStreamsSinkBuilder}
     */
    public static <InputT> KinesisDataStreamsSinkBuilder<InputT> builder() {
        return new KinesisDataStreamsSinkBuilder<>();
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
}
