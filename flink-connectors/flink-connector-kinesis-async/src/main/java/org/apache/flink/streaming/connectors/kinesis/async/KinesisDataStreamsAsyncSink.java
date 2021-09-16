package org.apache.flink.streaming.connectors.kinesis.async;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** a. */
public class KinesisDataStreamsAsyncSink<InputT>
        extends AsyncSinkBase<InputT, PutRecordsRequestEntry> {

    private final ElementConverter<InputT, PutRecordsRequestEntry> elementConverter;

    public KinesisDataStreamsAsyncSink(
            ElementConverter<InputT, PutRecordsRequestEntry> elementConverter) {
        this.elementConverter = elementConverter;
    }

    @Override
    public SinkWriter<InputT, Void, Collection<PutRecordsRequestEntry>> createWriter(
            InitContext context, List<Collection<PutRecordsRequestEntry>> states) {
        return new KinesisDataStreamsAsyncSinkWriter<>(
                elementConverter, context, 10, 1, 100, 1024, 10000);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<PutRecordsRequestEntry>>>
            getWriterStateSerializer() {
        return Optional.empty();
    }
}
