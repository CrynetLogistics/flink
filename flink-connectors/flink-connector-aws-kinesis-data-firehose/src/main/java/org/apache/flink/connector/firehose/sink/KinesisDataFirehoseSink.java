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

package org.apache.flink.connector.firehose.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.firehose.model.Record;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * A Kinesis Data Firehose (KDF) Sink that performs async requests against a destination delivery
 * stream using the buffering protocol specified in {@link AsyncSinkBase}.
 *
 * <p>The sink internally uses a {@link
 * software.amazon.awssdk.services.firehose.FirehoseAsyncClient} to communicate with the AWS
 * endpoint.
 *
 * <p>The behaviour of the buffering may be specified by providing configuration during the sink
 * build time.
 *
 * <ul>
 *   <li>{@code maxBatchSize}: the maximum size of a batch of entries that may be sent to KDF
 *   <li>{@code maxInFlightRequests}: the maximum number of in flight requests that may exist, if
 *       any more in flight requests need to be initiated once the maximum has been reached, then it
 *       will be blocked until some have completed
 *   <li>{@code maxBufferedRequests}: the maximum number of elements held in the buffer, requests to
 *       add elements will be blocked while the number of elements in the buffer is at the maximum
 *   <li>{@code maxBatchSizeInBytes}: the maximum size of a batch of entries that may be sent to KDF
 *       measured in bytes
 *   <li>{@code maxTimeInBufferMS}: the maximum amount of time an entry is allowed to live in the
 *       buffer, if any element reaches this age, the entire buffer will be flushed immediately
 *   <li>{@code maxRecordSizeInBytes}: the maximum size of a record the sink will accept into the
 *       buffer, a record of size larger than this will be rejected when passed to the sink
 *   <li>{@code failOnError}: when an exception is encountered while persisting to Kinesis Data
 *       Firehose, the job will fail immediately if failOnError is set
 * </ul>
 *
 * <p>Please see the writer implementation in {@link KinesisDataFirehoseSinkWriter}
 *
 * @param <InputT> Type of the elements handled by this sink
 */
@PublicEvolving
public class KinesisDataFirehoseSink<InputT> extends AsyncSinkBase<InputT, Record> {

    private final boolean failOnError;
    private final String deliveryStreamName;
    private final Properties kinesisClientProperties;

    KinesisDataFirehoseSink(
            ElementConverter<InputT, Record> elementConverter,
            Integer maxBatchSize,
            Integer maxInFlightRequests,
            Integer maxBufferedRequests,
            Long maxBatchSizeInBytes,
            Long maxTimeInBufferMS,
            Long maxRecordSizeInBytes,
            boolean failOnError,
            String deliveryStreamName,
            Properties kinesisClientProperties) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.deliveryStreamName =
                Preconditions.checkNotNull(
                        deliveryStreamName,
                        "The delivery stream name must not be null when initializing the KDF Sink.");
        Preconditions.checkArgument(
                !this.deliveryStreamName.isEmpty(),
                "The delivery stream name must be set when initializing the KDF Sink.");
        this.failOnError = failOnError;
        this.kinesisClientProperties = kinesisClientProperties;
    }

    /**
     * Create a {@link KinesisDataFirehoseSinkBuilder} to allow the fluent construction of a new
     * {@code KinesisDataFirehoseSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link KinesisDataFirehoseSinkBuilder}
     */
    public static <InputT> KinesisDataFirehoseSinkBuilder<InputT> builder() {
        return new KinesisDataFirehoseSinkBuilder<>();
    }

    @Override
    public SinkWriter<InputT, Void, Collection<Record>> createWriter(
            InitContext context, List<Collection<Record>> states) {
        return new KinesisDataFirehoseSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                failOnError,
                deliveryStreamName,
                kinesisClientProperties);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<Record>>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
