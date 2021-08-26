package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.Sink;

import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AsyncSinkWriterTest {

    private final List<Integer> res = new ArrayList<>();
    private final SinkInitContext sinkInitContext = new SinkInitContext();

    @Before
    public void before() {
        res.clear();
    }

    @Test
    public void numOfRecordsIsAMultipleOfBatchSizeResultsInThatNumberOfRecordsBeingWritten()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100);
        for (int i = 0; i < 110; i++) {
            sink.write(String.valueOf(i), null);
        }
        assertEquals(110, res.size());
    }

    @Test
    public void unwrittenRecordsInBufferArePersistedWhenSnapshotIsTaken()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100);
        for (int i = 0; i < 23; i++) {
            sink.write(String.valueOf(i), null);
        }
        assertEquals(20, res.size());
        assertEquals(List.of(20, 21, 22), new ArrayList<>(sink.snapshotState().get(0)));
    }

    @Test
    public void preparingCommitAtSnapshotTimeEnsuresTheBufferedRecordsArePersistedToDestination()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100);
        for (int i = 0; i < 23; i++) {
            sink.write(String.valueOf(i), null);
        }
        sink.prepareCommit(true);
        assertEquals(23, res.size());
    }

    private class AsyncSinkWriterImpl extends AsyncSinkWriter<String, Integer> {

        public AsyncSinkWriterImpl(
                Sink.InitContext context, int maxBatchSize,
                int maxInFlightRequests, int maxBufferedRequests) {
            super((elem, ctx) -> Integer.parseInt(elem),
                    context, maxBatchSize, maxInFlightRequests, maxBufferedRequests);
        }

        @Override
        protected void submitRequestEntries(
                List<Integer> requestEntries,
                ResultFuture<Integer> requestResult) {
            res.addAll(requestEntries);
            requestResult.complete(new ArrayList<>());
        }
    }

    private static class SinkInitContext implements Sink.InitContext {

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            StreamTaskActionExecutor streamTaskActionExecutor = new StreamTaskActionExecutor() {
                @Override
                public void run(RunnableWithException e) throws Exception {
                    e.run();
                }

                @Override
                public <E extends Throwable> void runThrowing(ThrowingRunnable<E> throwingRunnable) throws E {
                    throwingRunnable.run();
                }

                @Override
                public <R> R call(Callable<R> callable) throws Exception {
                    return callable.call();
                }
            };
            return new MailboxExecutorImpl(
                    new TaskMailboxImpl(Thread.currentThread()),
                    10,
                    streamTaskActionExecutor);
        }

        @Override
        public Sink.ProcessingTimeService getProcessingTimeService() {
            return null;
        }

        @Override
        public int getSubtaskId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 0;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return null;
        }
    }
}
