package org.apache.flink.connector.base.sink;


import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ResultFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;


/** Dummy destination that records puts and generate failed request occasionally. */
public class ArrayListAsyncSink extends AsyncSinkBase<String, Integer> {

    @Override
    public SinkWriter<String, Void, Collection<Integer>> createWriter(
            InitContext context, List<Collection<Integer>> states) {
        return new ArrayListAsyncSinkWriter(context);
    }

}
