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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponse;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponseEntry;
import software.amazon.awssdk.services.firehose.model.Record;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Sink writer created by {@link KinesisDataFirehoseSink} to write to Kinesis Data Streams. More
 * details on the operation of this sink writer may be found in the doc for {@link
 * KinesisDataFirehoseSink}. More details on the internals of this sink writer may be found in {@link
 * AsyncSinkWriter}.
 *
 * <p>The {@link FirehoseAsyncClient} used here may be configured in the standard way for the AWS SDK
 * 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and {@code
 * AWS_SECRET_ACCESS_KEY} through environment variables etc.
 */
class KinesisDataFirehoseSinkWriter<InputT> extends AsyncSinkWriter<InputT, Record> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisDataFirehoseSinkWriter.class);

    /* A counter for the total number of records that have encountered an error during put */
    private final Counter numRecordsOutErrorsCounter;

    /* Name of the stream in Kinesis Data Streams */
    private final String streamName;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    /* The asynchronous Kinesis client - construction is by kinesisClientProperties */
    private final FirehoseAsyncClient client;

    /* Flag to whether fatally fail any time we encounter an exception when persisting records */
    private final boolean failOnError;

    KinesisDataFirehoseSinkWriter(
            ElementConverter<InputT, Record> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String streamName,
            Properties kinesisClientProperties) {
        super(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.failOnError = failOnError;
        this.streamName = streamName;
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.client = buildClient(kinesisClientProperties);
    }

    private FirehoseAsyncClient buildClient(Properties kinesisClientProperties) {
        //FirehoseAsyncClientBuilder builder = FirehoseAsyncClient.builder();

        final SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(kinesisClientProperties);

        return FirehoseAsyncClient.create();
    }

    @Override
    protected void submitRequestEntries(
            List<Record> requestEntries,
            Consumer<Collection<Record>> requestResult) {

        PutRecordBatchRequest batchRequest =
                PutRecordBatchRequest.builder().records(requestEntries).deliveryStreamName(streamName).build();

        LOG.trace("Request to submit {} entries to KDS using KDS Sink.", requestEntries.size());

        CompletableFuture<PutRecordBatchResponse> future = client.putRecordBatch(batchRequest);

        future.whenComplete(
                (response, err) -> {
                    if (err != null) {
                        handleFullyFailedRequest(err, requestEntries, requestResult);
                    } else if (response.failedPutCount() > 0) {
                        handlePartiallyFailedRequest(response, requestEntries, requestResult);
                    } else {
                        requestResult.accept(Collections.emptyList());
                    }
                });
    }

    @Override
    protected long getSizeInBytes(Record requestEntry) {
        return requestEntry.data().asByteArrayUnsafe().length;
    }

    private void handleFullyFailedRequest(
            Throwable err,
            List<Record> requestEntries,
            Consumer<Collection<Record>> requestResult) {
        LOG.warn(
                "KDS Sink failed to persist {} entries to KDS first request was {}",
                requestEntries.size(),
                requestEntries.get(0).toString(),
                err);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (isRetryable(err)) {
            requestResult.accept(requestEntries);
        }
    }

    private void handlePartiallyFailedRequest(
            PutRecordBatchResponse response,
            List<Record> requestEntries,
            Consumer<Collection<Record>> requestResult) {
        LOG.warn(
                "KDS Sink failed to persist {} entries to KDS first request was {}",
                requestEntries.size(),
                requestEntries.get(0).toString());
        numRecordsOutErrorsCounter.inc(response.failedPutCount());

        if (failOnError) {
            getFatalExceptionCons()
                    .accept(new KinesisDataFirehoseException.KinesisDataStreamsFailFastException());
            return;
        }
        List<Record> failedRequestEntries =
                new ArrayList<>(response.failedPutCount());
        List<PutRecordBatchResponseEntry> records = response.requestResponses();

        for (int i = 0; i < records.size(); i++) {
            if (records.get(i).errorCode() != null) {
                failedRequestEntries.add(requestEntries.get(i));
            }
        }

        requestResult.accept(failedRequestEntries);
    }

    private boolean isRetryable(Throwable err) {
        if (err instanceof CompletionException
                && err.getCause() instanceof ResourceNotFoundException) {
            getFatalExceptionCons()
                    .accept(
                            new KinesisDataFirehoseException(
                                    "Encountered non-recoverable exception", err));
            return false;
        }
        if (failOnError) {
            getFatalExceptionCons()
                    .accept(
                            new KinesisDataFirehoseException.KinesisDataStreamsFailFastException(
                                    err));
            return false;
        }

        return true;
    }
}
