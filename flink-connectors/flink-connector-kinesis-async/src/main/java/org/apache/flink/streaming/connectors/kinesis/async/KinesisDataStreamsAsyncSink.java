package org.apache.flink.streaming.connectors.kinesis.async;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class KinesisDataStreamsAsyncSink<InputT, RequestEntryT extends Serializable>
        extends AsyncSinkBase<InputT, RequestEntryT> {

    private ElementConverter<InputT, RequestEntryT> elementConverter;

    public KinesisDataStreamsAsyncSink(ElementConverter<InputT, RequestEntryT> elementConverter){
        this.elementConverter = elementConverter;
    }

    @Override
    public SinkWriter<InputT, Void, Collection<RequestEntryT>> createWriter(
            InitContext context,
            List<Collection<RequestEntryT>> states) throws IOException {
        return new KinesisDataStreamsAsyncSinkWriter<InputT, RequestEntryT>(
                elementConverter, context, 10, 1,
                100, 1024, 10000);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<RequestEntryT>>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
