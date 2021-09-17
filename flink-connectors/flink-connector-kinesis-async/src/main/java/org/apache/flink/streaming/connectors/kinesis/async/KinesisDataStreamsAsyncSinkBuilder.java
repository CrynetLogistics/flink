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

/** a. */
@PublicEvolving
public class KinesisDataStreamsAsyncSinkBuilder<InputT> {

    private ElementConverter<InputT, PutRecordsRequestEntry> elementConverter;
    private int maxBatchSize;
    private int maxInFlightRequests;
    private int maxBufferedRequests;
    private long flushOnBufferSizeInBytes;
    private long maxTimeInBufferMS;

    public KinesisDataStreamsAsyncSinkBuilder<InputT> setElementConverter(
            ElementConverter<InputT, PutRecordsRequestEntry> elementConverter) {
        this.elementConverter = elementConverter;
        return this;
    }

    public KinesisDataStreamsAsyncSinkBuilder<InputT> setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    public KinesisDataStreamsAsyncSinkBuilder<InputT> setMaxInFlightRequests(int maxInFlightRequests) {
        this.maxInFlightRequests = maxInFlightRequests;
        return this;
    }

    public KinesisDataStreamsAsyncSinkBuilder<InputT> setMaxBufferedRequests(int maxBufferedRequests) {
        this.maxBufferedRequests = maxBufferedRequests;
        return this;
    }

    public KinesisDataStreamsAsyncSinkBuilder<InputT> setFlushOnBufferSizeInBytes(long flushOnBufferSizeInBytes) {
        this.flushOnBufferSizeInBytes = flushOnBufferSizeInBytes;
        return this;
    }

    public KinesisDataStreamsAsyncSinkBuilder<InputT> setMaxTimeInBufferMS(long maxTimeInBufferMS) {
        this.maxTimeInBufferMS = maxTimeInBufferMS;
        return this;
    }

    public KinesisDataStreamsAsyncSink<InputT> build(){
        return new KinesisDataStreamsAsyncSink<>(elementConverter, maxBatchSize,
                maxInFlightRequests, maxBufferedRequests, flushOnBufferSizeInBytes,
                maxTimeInBufferMS);
    }
}
