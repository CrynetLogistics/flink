package org.apache.flink.streaming.connectors.kinesis.async;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

public class KinesisDataStreamsAsyncSinkWriter<InputT, RequestEntryT extends Serializable>
        extends AsyncSinkWriter<InputT, RequestEntryT> {

    public KinesisDataStreamsAsyncSinkWriter(
            ElementConverter<InputT, RequestEntryT> elementConverter,
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
            List<RequestEntryT> requestEntries,
            Consumer<Collection<RequestEntryT>> requestResult) {
        // TODO: write to Kinesis Data Streams
    }

    @Override
    protected long getSizeInBytes(RequestEntryT requestEntry) {
        return 0;
    }
}
