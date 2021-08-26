package org.apache.flink.connector.base.sink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;

import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests Async Sink. */
public class AsyncSinkBaseITCase {

    private class MapWithOccasionalFailure extends RichMapFunction<Long, String> {

        private final boolean simulateFailures;

        public MapWithOccasionalFailure(boolean simulateFailures) {
            this.simulateFailures = simulateFailures;
        }

        @Override
        public String map(Long value) throws Exception {
            //if (getRuntimeContext().getIndexOfThisSubtask() == 0 &&
            //        getRuntimeContext().getAttemptNumber() == 0 && simulateFailures) {
            //    throw new RuntimeException("An intentional error occurred");
            //}
            return value.toString();
        }
    }

    @Test
    public void writeSingleDataToSink() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(20);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.milliseconds(200)));

        env
                .fromSequence(0, 100)
                .map(new MapWithOccasionalFailure(true))
                .sinkTo(new ArrayListAsyncSink());

        env.execute("Integration Test: AsyncSinkBaseITCase");
        System.out.println(ArrayListDestination.getStore());
    }

    @Test
    public void consistencyIssue() throws Exception {
        for(int i=0; i<5; i++){
            System.out.print(nonSerialisabilityIsAnIssue() + " ");
            ArrayListDestination.clearStore();
        }
    }

    public int nonSerialisabilityIsAnIssue() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(0, Time.milliseconds(200)));

        ArrayListAsyncSink sink = new ArrayListAsyncSink();
        env
                .fromSequence(0, 2000)
                //.forceNonParallel()
                .map(new MapWithOccasionalFailure(false))
                //.forceNonParallel()
                .sinkTo(sink);

        //JobExecutionResult jer = env.execute("Integration Test: AsyncSinkBaseITCase").getJobExecutionResult();
        env.execute("Integration Test: AsyncSinkBaseITCase").getJobExecutionResult();
        //System.out.println(jer.toString());
        //System.out.println(ArrayListDestination.getStore().size());
        //Thread.sleep(3000);
        //System.out.println(ArrayListDestination.getStore().stream().sorted().collect(Collectors.toList()));
        return ArrayListDestination.getStore();
    }


}
