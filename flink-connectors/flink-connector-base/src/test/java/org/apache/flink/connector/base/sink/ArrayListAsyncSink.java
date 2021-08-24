package org.apache.flink.connector.base.sink;


import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;


/** Dummy destination that records puts and generate failed request occasionally. */
public class ArrayListAsyncSink extends AsyncSinkBase<String, Integer> {

    @Override
    public SinkWriter<String, Void, Collection<Integer>> createWriter(
            InitContext context, List<Collection<Integer>> states) {
        return new ArrayListAsyncSinkWriter(context);
    }

}
