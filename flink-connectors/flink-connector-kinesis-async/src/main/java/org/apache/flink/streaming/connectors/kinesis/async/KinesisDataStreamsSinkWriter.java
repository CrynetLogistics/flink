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
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 *  Sink writer created by {@link KinesisDataStreamsSink} to write to Kinesis Data Streams. More
 *  details on the operation of this sink writer may be found in the doc for {@link KinesisDataStreamsSink}.
 *  More details on the internals of this sink writer may be found in {@link AsyncSinkWriter}.
 *
 *  The {@link KinesisAsyncClient} used here may be configured in the standard way for the
 *  AWS SDK 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and
 *  {@code AWS_SECRET_ACCESS_KEY} through environment variables etc.
 */
@PublicEvolving
public class KinesisDataStreamsSinkWriter<InputT>
        extends AsyncSinkWriter<InputT, PutRecordsRequestEntry> {

    private static final KinesisAsyncClient client = KinesisAsyncClient.create();

    public KinesisDataStreamsSinkWriter(
            ElementConverter<InputT, PutRecordsRequestEntry> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long flushOnBufferSizeInBytes,
            long maxTimeInBufferMS) {
        super(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                flushOnBufferSizeInBytes,
                maxTimeInBufferMS);
    }

    @Override
    protected void submitRequestEntries(
            List<PutRecordsRequestEntry> requestEntries,
            Consumer<Collection<PutRecordsRequestEntry>> requestResult) {

        // create a batch request
        PutRecordsRequest batchRequest =
                PutRecordsRequest.builder().records(requestEntries).streamName("py-output").build();

        System.out.println("submitRequestEntries: putRecords");

        // call api with batch request
        CompletableFuture<PutRecordsResponse> future = client.putRecords(batchRequest);

        // re-queue elements of failed requests
        future.whenComplete(
                (response, err) -> {
                    if (err != null) {
                        System.out.printf("kinesis:PutRecords request failed: %s", err);

                        requestResult.accept(requestEntries);

                        return;
                    }

                    if (response.failedRecordCount() > 0) {
                        System.out.printf(
                                "Re-queueing {} messages%s", response.failedRecordCount());

                        ArrayList<PutRecordsRequestEntry> failedRequestEntries =
                                new ArrayList<>(response.failedRecordCount());
                        List<PutRecordsResultEntry> records = response.records();

                        for (int i = 0; i < records.size(); i++) {
                            if (records.get(i).errorCode() != null) {
                                failedRequestEntries.add(requestEntries.get(i));
                            }
                        }

                        requestResult.accept(failedRequestEntries);
                    } else {
                        requestResult.accept(Collections.emptyList());
                    }
                });
    }

    @Override
    protected long getSizeInBytes(PutRecordsRequestEntry requestEntry) {
        return requestEntry.data().asByteArrayUnsafe().length;
    }
}
