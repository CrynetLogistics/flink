package org.apache.flink.streaming.connectors.kinesis.async;

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

/** a. */
public class KinesisDataStreamsAsyncSinkWriter<InputT>
        extends AsyncSinkWriter<InputT, PutRecordsRequestEntry> {

    private static final KinesisAsyncClient client = KinesisAsyncClient.create();

    public KinesisDataStreamsAsyncSinkWriter(
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
                        System.out.printf("kinesis:PutRecords request failed: %n", err);

                        requestResult.accept(requestEntries);

                        return;
                    }

                    if (response.failedRecordCount() > 0) {
                        System.out.printf(
                                "Re-queueing {} messages%n", response.failedRecordCount());

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
