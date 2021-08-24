package org.apache.flink.connector.base.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;

/** Tests Async Sink. */
public class AsyncSinkBaseITCase {

    @Test
    public void writeSingleDataToSink() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(20);

        DataStream<String> source = env.fromSequence(0, 10).map(Object::toString);

        source.sinkTo(new ArrayListAsyncSink());

        env.execute("Integration Test: AsyncSinkBaseITCase");
    }
}
