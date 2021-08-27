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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext,
                10, 1, 100, false);
        for (int i = 0; i < 80; i++) {
            sink.write(String.valueOf(i), null);
        }
        assertEquals(80, res.size());
    }

    @Test
    public void unwrittenRecordsInBufferArePersistedWhenSnapshotIsTaken()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext,
                10, 1, 100, false);
        for (int i = 0; i < 23; i++) {
            sink.write(String.valueOf(i), null);
        }
        assertEquals(20, res.size());
        assertEquals(List.of(20, 21, 22), new ArrayList<>(sink.snapshotState().get(0)));
    }

    @Test
    public void preparingCommitAtSnapshotTimeEnsuresTheBufferedRecordsArePersistedToDestination()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext,
                10, 1, 100, false);
        for (int i = 0; i < 23; i++) {
            sink.write(String.valueOf(i), null);
        }
        sink.prepareCommit(true);
        assertEquals(23, res.size());
    }

    @Test
    public void snapshotsAreTakenOfBufferCorrectlyBeforeAndAfterManualAndAutomaticFlush()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext,
                3, 1, 100, false);

        sink.write("25", null);
        sink.write("55", null);
        assertEquals(List.of(25, 55), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(0, res.size());

        sink.write("75", null);
        assertEquals(List.of(), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(3, res.size());

        sink.write("95", null);
        sink.write("955", null);
        assertEquals(List.of(95, 955), new ArrayList<>(sink.snapshotState().get(0)));
        sink.prepareCommit(true);
        assertEquals(List.of(), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(5, res.size());
    }

    @Test
    public void runtimeErrorsInSubmitRequestEntriesEndUpAsIOExceptionsWithNumberOfFailedRequests()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext,
                3, 1, 100, false);
        sink.write("25", null);
        sink.write("55", null);
        sink.write("75", null);
        sink.write("95", null);
        sink.write("125", null);
        Exception e =
                assertThrows(IOException.class, () -> sink.write("135", null));
        assertEquals("Failed to submit up to [3] request entries, POSSIBLE DATA LOSS. A "
                + "runtime exception occured during the submission of the request entries",
                e.getMessage());
        assertEquals("Deliberate runtime exception occurred in SinkWriterImplementation.",
                e.getCause().getMessage());

        sink.prepareCommit(true);
        assertEquals(3, res.size());
    }

    @Test
    public void nonRuntimeErrorsDoNotResultInViolationOfAtLeastOnceSemanticsDueToRequeueOfFailures()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext,
                3, 1, 100, false);
        sink.write("25", null);
        sink.write("55", null);
        sink.write("75", null);
        sink.write("95", null);
        sink.write("955", null);
        sink.write("550", null);
        sink.write("45", null);
        sink.write("545", null);

        assertEquals(6, res.size());
        assertEquals(List.of(45, 545), new ArrayList<>(sink.snapshotState().get(0)));

        sink.prepareCommit(true);
        assertEquals(8, res.size());
    }


    private class AsyncSinkWriterImpl extends AsyncSinkWriter<String, Integer> {

        private final Set<Integer> failedFirstAttempts = new HashSet<>();
        private final boolean simulateFailures;

        public AsyncSinkWriterImpl(
                Sink.InitContext context, int maxBatchSize,
                int maxInFlightRequests, int maxBufferedRequests, boolean simulateFailures) {
            super((elem, ctx) -> Integer.parseInt(elem),
                    context, maxBatchSize, maxInFlightRequests, maxBufferedRequests);
            this.simulateFailures = simulateFailures;
        }

        /**
         * Fails if any value is between 101 and 200, and if {@code simulateFailures} is set,
         * fails on the first attempt but succeeds upon retry on all others
         * @param requestEntries a set of request entries that should be persisted to {@code res}
         * @param requestResult a ResultFuture that needs to be completed once all request entries
         *     have been persisted. Any failures should be elements of the list being completed
         */
        @Override
        protected void submitRequestEntries(
                List<Integer> requestEntries,
                ResultFuture<Integer> requestResult) {
            if(requestEntries.stream().anyMatch(val -> val > 100 && val <= 200)) {
                throw new RuntimeException(
                        "Deliberate runtime exception occurred in SinkWriterImplementation.");
            }
            if(simulateFailures) {
                List<Integer> successfulRetries = failedFirstAttempts.stream()
                        .filter(requestEntries::contains).collect(Collectors.toList());
                requestEntries.removeIf(successfulRetries::contains);
                failedFirstAttempts.removeIf(successfulRetries::contains);

                List<Integer> firstTimeFailed = requestEntries
                        .stream().filter(val -> val > 200).collect(Collectors.toList());
                requestEntries.removeAll(firstTimeFailed);

                res.addAll(requestEntries);
                requestResult.complete(firstTimeFailed);
            }
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
