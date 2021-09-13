/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit Tests the functionality of AsyncSinkWriter without any assumptions of what a concrete
 * implementation might do.
 */
public class AsyncSinkWriterTest {

    private final List<Integer> res = new ArrayList<>();
    private final SinkInitContext sinkInitContext = new SinkInitContext();

    @Before
    public void before() {
        res.clear();
    }

    @Test
    public void testNumberOfRecordsIsAMultipleOfBatchSizeResultsInThatNumberOfRecordsBeingWritten()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100, false);
        for (int i = 0; i < 80; i++) {
            sink.write(String.valueOf(i));
        }
        assertEquals(80, res.size());
    }

    @Test
    public void testThatUnwrittenRecordsInBufferArePersistedWhenSnapshotIsTaken()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100, false);
        for (int i = 0; i < 23; i++) {
            sink.write(String.valueOf(i));
        }
        assertEquals(20, res.size());
        assertEquals(Arrays.asList(20, 21, 22), new ArrayList<>(sink.snapshotState().get(0)));
    }

    @Test
    public void testPreparingCommitAtSnapshotTimeEnsuresBufferedRecordsArePersistedToDestination()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100, false);
        for (int i = 0; i < 23; i++) {
            sink.write(String.valueOf(i));
        }
        sink.prepareCommit(true);
        assertEquals(23, res.size());
    }

    @Test
    public void testThatMailboxYieldDoesNotBlockWhileATimerIsRegisteredAndHasYetToElapse()
            throws Exception {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100, false);
        sink.write(String.valueOf(0));
        sink.prepareCommit(true);
        assertEquals(1, res.size());
    }

    @Test
    public void testThatSnapshotsAreTakenOfBufferCorrectlyBeforeAndAfterAutomaticFlush()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, false);

        sink.write("25");
        sink.write("55");
        assertEquals(Arrays.asList(25, 55), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(0, res.size());

        sink.write("75");
        assertEquals(Arrays.asList(), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(3, res.size());
    }

    @Test
    public void testThatSnapshotsAreTakenOfBufferCorrectlyBeforeAndAfterManualFlush()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, false);
        sink.write("25");
        sink.write("55");
        sink.write("75");
        sink.write("95");
        sink.write("955");
        assertEquals(Arrays.asList(95, 955), new ArrayList<>(sink.snapshotState().get(0)));
        sink.prepareCommit(true);
        assertEquals(Arrays.asList(), new ArrayList<>(sink.snapshotState().get(0)));
        assertEquals(5, res.size());
    }

    @Test
    public void testRuntimeErrorsInSubmitRequestEntriesEndUpAsIOExceptionsWithNumOfFailedRequests()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, true);
        sink.write("25");
        sink.write("55");
        sink.write("75");
        sink.write("95");
        sink.write("35");
        Exception e = assertThrows(RuntimeException.class, () -> sink.write("135"));
        assertEquals(
                "Deliberate runtime exception occurred in SinkWriterImplementation.",
                e.getMessage());
        assertEquals(3, res.size());
    }

    @Test
    public void testRetryableErrorsDoNotViolateAtLeastOnceSemanticsDueToRequeueOfFailures()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, true);

        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "25", Arrays.asList(), Arrays.asList(25));

        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "55", Arrays.asList(), Arrays.asList(25, 55));

        // 25, 55 persisted; 965 failed and inflight
        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "965", Arrays.asList(25, 55), Arrays.asList());

        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "75", Arrays.asList(25, 55), Arrays.asList(75));

        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "95", Arrays.asList(25, 55), Arrays.asList(75, 95));

        /*
         * Writing 955 to the sink increases the buffer to size 3 containing [75, 95, 955]. This
         * triggers the outstanding in flight request with the failed 965 to be run, and 965 is
         * placed at the front of the queue. The first {@code maxBatchSize = 3} elements are
         * persisted, with 965 succeeding this (second) time. 955 remains in the buffer.
         */
        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "955", Arrays.asList(25, 55, 965, 75, 95), Arrays.asList(955));

        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "550", Arrays.asList(25, 55, 965, 75, 95), Arrays.asList(955, 550));

        /*
         * [955, 550, 45] are attempted to be persisted
         */
        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "45", Arrays.asList(25, 55, 965, 75, 95, 45), Arrays.asList());

        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "35", Arrays.asList(25, 55, 965, 75, 95, 45), Arrays.asList(35));

        /* [35, 535] should be in the bufferedRequestEntries
         * [955, 550] should be in the inFlightRequest, ready to be added
         * [25, 55, 965, 75, 95, 45] should be downstream already
         */
        writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
                sink, "535", Arrays.asList(25, 55, 965, 75, 95, 45), Arrays.asList(35, 535));

        // Checkpoint occurs
        sink.prepareCommit(true);

        // Everything is saved
        assertEquals(Arrays.asList(25, 55, 965, 75, 95, 45, 550, 955, 35, 535), res);
        assertEquals(0, sink.snapshotState().get(0).size());
    }

    @Test
    public void testFailedEntriesAreRetriedInTheNextPossiblePersistRequestAndNoLater()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(sinkInitContext, 3, 1, 100, true);
        sink.write("25");
        sink.write("55");
        sink.write("965");
        sink.write("75");
        sink.write("95");
        sink.write("955");
        assertTrue(res.contains(965));
        sink.write("550");
        sink.write("645");
        sink.write("545");
        sink.write("535");
        sink.write("515");
        assertTrue(res.contains(955));
        sink.write("505");
        assertTrue(res.contains(550));
        assertTrue(res.contains(645));
        sink.prepareCommit(true);
        assertTrue(res.contains(545));
        assertTrue(res.contains(535));
        assertTrue(res.contains(515));
    }

    @Test
    public void testThatMaxBufferSizeOfSinkShouldBeStrictlyGreaterThanMaxSizeOfEachBatch() {
        Exception e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 10, false));
        assertEquals(
                e.getMessage(),
                "The maximum number of requests that may be buffered should be "
                        + "strictly greater than the maximum number of requests per batch.");
    }

    private void writeXToSinkAssertDestinationIsInStateYAndBufferHasZ(
            AsyncSinkWriterImpl sink, String x, List<Integer> y, List<Integer> z)
            throws IOException, InterruptedException {
        sink.write(x);
        assertEquals(y, res);
        assertEquals(z, new ArrayList<>(sink.snapshotState().get(0)));
    }

    @Test
    public void testFlushThresholdMetBeforeBatchLimitWillCreateASmallerBatchOfSizeAboveThreshold()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink =
                new AsyncSinkWriterImpl(
                        sinkInitContext, 10, 1, 100, 30, 1000, false);

        /* Sink has flush threshold of 30 bytes, each integer is 4 bytes, therefore, flushing
         * should occur once 8 elements have been written.
         */
        for (int i = 0; i < 15; i++) {
            sink.write(String.valueOf(i));
        }
        assertEquals(8, res.size());
        sink.write(String.valueOf(15));
        assertEquals(16, res.size());
    }

    @Test
    public void testThatWhenNumberOfItemAndSizeOfRecordThresholdsAreMetSimultaneouslyAFlushOccurs()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink =
                new AsyncSinkWriterImpl(
                        sinkInitContext, 8, 1, 100, 32, 1000, false);

        for (int i = 0; i < 8; i++) {
            sink.write(String.valueOf(i));
        }
        assertEquals(8, res.size());
        for (int i = 8; i < 16; i++) {
            sink.write(String.valueOf(i));
        }
        assertEquals(16, res.size());
    }

    @Test
    public void testThatIntermittentlyFailingEntriesAreEnqueuedOnToTheBufferWithCorrectSize()
            throws IOException, InterruptedException {
        AsyncSinkWriterImpl sink =
                new AsyncSinkWriterImpl(
                        sinkInitContext, 10, 1, 100, 110, 1000, true);

        sink.write(String.valueOf(225)); // Buffer: 100/110B; 1/10 elements; 0 inflight
        sink.write(String.valueOf(1)); //   Buffer: 104/110B; 2/10 elements; 0 inflight
        sink.write(String.valueOf(2)); //   Buffer: 108/110B; 3/10 elements; 0 inflight
        sink.write(String.valueOf(3)); //   Buffer: 112/110B; 4/10 elements; 0 inflight -- flushing
        assertEquals(3, res.size()); // Element 225 failed on first attempt
        sink.write(String.valueOf(4)); //   Buffer:   4/110B; 1/10 elements; 1 inflight
        sink.write(String.valueOf(5)); //   Buffer:   8/110B; 2/10 elements; 1 inflight
        sink.write(String.valueOf(6)); //   Buffer:  12/110B; 3/10 elements; 1 inflight
        sink.write(String.valueOf(325)); // Buffer: 112/110B; 4/10 elements; 1 inflight -- flushing

        assertEquals(Arrays.asList(1, 2, 3, 225, 4, 5, 6), res);
    }

    @Test
    public void testThatABatchWithSizeSmallerThanMaxBatchSizeIsFlushedOnTimeoutExpiry()
            throws Exception {
        AsyncSinkWriterImpl sink =
                new AsyncSinkWriterImpl(sinkInitContext, 10, 20, 100, 10000, 100, true);

        TestProcessingTimeService tpts = sinkInitContext.getTestProcessingTimeService();
        tpts.setCurrentTime(0L);
        for (int i = 0; i < 8; i++) {
            sink.write(String.valueOf(i));
        }

        tpts.setCurrentTime(99L);
        assertEquals(0, res.size());
        tpts.setCurrentTime(100L);
        assertEquals(8, res.size());
    }

    @Test
    public void testThatTimeBasedBatchPicksUpAllRelevantItemsUpUntilExpiryOfTimer()
            throws Exception {
        AsyncSinkWriterImpl sink =
                new AsyncSinkWriterImpl(sinkInitContext, 10, 20, 100, 10000, 100, true);

        TestProcessingTimeService tpts = sinkInitContext.getTestProcessingTimeService();
        for (int i = 0; i < 98; i++) {
            tpts.setCurrentTime(i);
            sink.write(String.valueOf(i));
        }
        tpts.setCurrentTime(99L);
        assertEquals(90, res.size());
        tpts.setCurrentTime(100L);
        assertEquals(98, res.size());
    }

    @Test
    public void testThatOneAndOnlyOneCallbackIsEverRegistered() throws Exception {
        AsyncSinkWriterImpl sink =
                new AsyncSinkWriterImpl(sinkInitContext, 10, 20, 100, 10000, 100, true);

        TestProcessingTimeService tpts = sinkInitContext.getTestProcessingTimeService();
        tpts.setCurrentTime(0L);
        sink.write("1"); // A timer is registered here to elapse at t=100
        assertEquals(0, res.size());
        tpts.setCurrentTime(10L);
        sink.prepareCommit(true);
        assertEquals(1, res.size());
        tpts.setCurrentTime(20L); // At t=20, we write a new element that should not trigger another
        sink.write("2"); // timer to be registered. If it is, it should elapse at t=120s.
        assertEquals(1, res.size());
        tpts.setCurrentTime(100L);
        assertEquals(2, res.size());
        sink.write("3");
        tpts.setCurrentTime(199L); // At t=199s, our third element has not been written
        assertEquals(2, res.size()); // therefore, no timer fired at 120s.
        tpts.setCurrentTime(200L);
        assertEquals(3, res.size());
    }

    @Test
    public void testThatIntermittentlyFailingEntriesShouldBeFlushedWithMainBatchInTimeBasedFlush()
            throws Exception {
        AsyncSinkWriterImpl sink =
                new AsyncSinkWriterImpl(sinkInitContext, 10, 1, 100, 10000, 100, true);

        TestProcessingTimeService tpts = sinkInitContext.getTestProcessingTimeService();
        tpts.setCurrentTime(0L);
        sink.write("1");
        sink.write("2");
        sink.write("225");
        tpts.setCurrentTime(100L);
        assertEquals(2, res.size());
        sink.write("3");
        sink.write("4");
        tpts.setCurrentTime(199L);
        assertEquals(2, res.size());
        tpts.setCurrentTime(200L);
        assertEquals(5, res.size());
    }

    @Test
    public void testThatFlushingAnEmptyBufferDoesNotResultInErrorOrFailure() throws Exception {
        AsyncSinkWriterImpl sink =
                new AsyncSinkWriterImpl(sinkInitContext, 10, 20, 100, 10000, 100, true);

        TestProcessingTimeService tpts = sinkInitContext.getTestProcessingTimeService();
        tpts.setCurrentTime(0L);
        sink.write("1");
        tpts.setCurrentTime(50L);
        sink.prepareCommit(true);
        assertEquals(1, res.size());
        tpts.setCurrentTime(200L);
    }

    @Test
    public void testThatOnExpiryOfAnOldTimeoutANewOneMayBeRegisteredImmediately() throws Exception {
        AsyncSinkWriterImpl sink =
                new AsyncSinkWriterImpl(sinkInitContext, 10, 20, 100, 10000, 100, true);

        TestProcessingTimeService tpts = sinkInitContext.getTestProcessingTimeService();
        tpts.setCurrentTime(0L);
        sink.write("1");
        tpts.setCurrentTime(100L);
        assertEquals(1, res.size());
        sink.write("2");
        tpts.setCurrentTime(200L);
        assertEquals(2, res.size());
    }

    /**
     * This test considers what could happen if the timer elapses, triggering a flush, while a
     * long-running call to {@code submitRequestEntries} remains uncompleted for some time. We have
     * a countdown latch with an expiry of 500ms installed in the call to {@code
     * submitRequestEntries} that blocks if the batch size received is 3 and subsequently accepts
     * and succeeds with any value.
     *
     * <p>Let us call the thread writing "3" thread3 and the thread writing "4" thread4. Thread3
     * will enter {@code submitRequestEntries} with 3 entries and release thread4. Thread3 becomes
     * blocked for 500ms. Thread4 writes "4" to the buffer and is flushed when the timer triggers
     * (timer was first set when "1" was written). Thread4 then is blocked during the flush phase
     * since thread3 is in-flight and maxInFlightRequests=1. After 500ms elapses, thread3 is revived
     * and proceeds, which also unblocks thread4. This results in 1, 2, 3 being written prior to 4.
     *
     * <p>This test also implicitly asserts that any thread in the SinkWriter must be the mailbox
     * thread if it enters {@code mailbox.tryYield()}.
     */
    @Test
    public void testThatInterleavingThreadsMayBlockEachOtherButDoNotCauseRaceConditions()
            throws Exception {
        CountDownLatch blockedWriteLatch = new CountDownLatch(1);
        CountDownLatch delayedStartLatch = new CountDownLatch(1);
        AsyncSinkWriterImpl sink =
                new AsyncSinkReleaseAndBlockWriterImpl(
                        sinkInitContext,
                        3,
                        1,
                        20,
                        100,
                        100,
                        blockedWriteLatch,
                        delayedStartLatch,
                        true);

        writeTwoElementsAndInterleaveTheNextTwoElements(sink, blockedWriteLatch, delayedStartLatch);
        assertEquals(Arrays.asList(1, 2, 3, 4), res);
    }

    /**
     * This test considers what could happen if the timer elapses, triggering a flush, while a
     * long-running call to {@code submitRequestEntries} remains blocked. We have a countdown latch
     * that blocks permanently until freed once the timer based flush is complete.
     *
     * <p>Let us call the thread writing "3" thread3 and the thread writing "4" thread4. Thread3
     * will enter {@code submitRequestEntries} with 3 entries and release thread4. Thread3 becomes
     * blocked. Thread4 writes "4" to the buffer and is flushed when the timer triggers (timer was
     * first set when "1" was written). Thread4 completes and frees thread3. Thread3 is revived and
     * proceeds. This results in 4 being written prior to 1, 2, 3.
     *
     * <p>This test also implicitly asserts that any thread in the SinkWriter must be the mailbox
     * thread if it enters {@code mailbox.tryYield()}.
     */
    @Test
    public void testThatIfOneInterleavedThreadIsBlockedTheOtherThreadWillContinueAndCorrectlyWrite()
            throws Exception {
        CountDownLatch blockedWriteLatch = new CountDownLatch(1);
        CountDownLatch delayedStartLatch = new CountDownLatch(1);
        AsyncSinkWriterImpl sink =
                new AsyncSinkReleaseAndBlockWriterImpl(
                        sinkInitContext,
                        3,
                        2,
                        20,
                        100,
                        100,
                        blockedWriteLatch,
                        delayedStartLatch,
                        false);

        writeTwoElementsAndInterleaveTheNextTwoElements(sink, blockedWriteLatch, delayedStartLatch);
        assertEquals(new ArrayList<>(Arrays.asList(4, 1, 2, 3)), res);
    }

    private void writeTwoElementsAndInterleaveTheNextTwoElements(
            AsyncSinkWriterImpl sink,
            CountDownLatch blockedWriteLatch,
            CountDownLatch delayedStartLatch)
            throws Exception {

        TestProcessingTimeService tpts = sinkInitContext.getTestProcessingTimeService();
        ExecutorService es = Executors.newFixedThreadPool(4);

        tpts.setCurrentTime(0L);
        sink.write("1");
        sink.write("2");
        es.submit(
                () -> {
                    try {
                        sink.write("3");
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });

        delayedStartLatch.await();
        sink.write("4");
        tpts.setCurrentTime(100L);
        blockedWriteLatch.countDown();
        es.shutdown();
        assertTrue(
                es.awaitTermination(500, TimeUnit.MILLISECONDS),
                "Executor Service stuck at termination, not terminated after 500ms!");
    }

    private class AsyncSinkWriterImpl extends AsyncSinkWriter<String, Integer> {

        private final Set<Integer> failedFirstAttempts = new HashSet<>();
        private final boolean simulateFailures;

        public AsyncSinkWriterImpl(
                Sink.InitContext context,
                int maxBatchSize,
                int maxInFlightRequests,
                int maxBufferedRequests,
                boolean simulateFailures) {
            super(
                    (elem, ctx) -> Integer.parseInt(elem),
                    context,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    10000000,
                    1000);
            this.simulateFailures = simulateFailures;
        }

        public AsyncSinkWriterImpl(
                Sink.InitContext context,
                int maxBatchSize,
                int maxInFlightRequests,
                int maxBufferedRequests,
                long flushOnBufferSizeInBytes,
                long maxTimeInBufferMS,
                boolean simulateFailures) {
            super(
                    (elem, ctx) -> Integer.parseInt(elem),
                    context,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    flushOnBufferSizeInBytes,
                    maxTimeInBufferMS);
            this.simulateFailures = simulateFailures;
        }

        public void write(String val) throws IOException, InterruptedException {
            write(val, null);
        }

        /**
         * Fails if any value is between 101 and 200. If {@code simulateFailures} is set, it will
         * fail on the first attempt but succeeds upon retry on all others for entries strictly
         * greater than 200.
         *
         * <p>A limitation of this basic implementation is that each element written must be unique.
         *
         * @param requestEntries a set of request entries that should be persisted to {@code res}
         * @param requestResult a Consumer that needs to accept a collection of failure elements
         *     once all request entries have been persisted
         */
        @Override
        protected void submitRequestEntries(
                List<Integer> requestEntries, Consumer<Collection<Integer>> requestResult) {
            if (requestEntries.stream().anyMatch(val -> val > 100 && val <= 200)) {
                throw new RuntimeException(
                        "Deliberate runtime exception occurred in SinkWriterImplementation.");
            }
            if (simulateFailures) {
                List<Integer> successfulRetries =
                        failedFirstAttempts.stream()
                                .filter(requestEntries::contains)
                                .collect(Collectors.toList());
                failedFirstAttempts.removeIf(successfulRetries::contains);

                List<Integer> firstTimeFailed =
                        requestEntries.stream()
                                .filter(x -> !successfulRetries.contains(x))
                                .filter(val -> val > 200)
                                .collect(Collectors.toList());
                failedFirstAttempts.addAll(firstTimeFailed);

                requestEntries.removeAll(firstTimeFailed);
                res.addAll(requestEntries);
                requestResult.accept(firstTimeFailed);
            } else {
                res.addAll(requestEntries);
                requestResult.accept(new ArrayList<>());
            }
        }

        /**
         * If we're simulating failures and the requestEntry value is greater than 200, then the
         * entry is size 100 bytes, otherwise each entry is 4 bytes.
         * @return
         */
        @Override
        protected long getSizeInBytes(Integer requestEntry) {
            return requestEntry > 200 && simulateFailures ? 100 : 4;
        }
    }

    private static class SinkInitContext implements Sink.InitContext {

        private static final TestProcessingTimeService processingTimeService;

        static {
            processingTimeService = new TestProcessingTimeService();
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            StreamTaskActionExecutor streamTaskActionExecutor =
                    new StreamTaskActionExecutor() {
                        @Override
                        public void run(RunnableWithException e) throws Exception {
                            e.run();
                        }

                        @Override
                        public <E extends Throwable> void runThrowing(
                                ThrowingRunnable<E> throwingRunnable) throws E {
                            throwingRunnable.run();
                        }

                        @Override
                        public <R> R call(Callable<R> callable) throws Exception {
                            return callable.call();
                        }
                    };
            return new MailboxExecutorImpl(
                    new TaskMailboxImpl(Thread.currentThread()),
                    Integer.MAX_VALUE,
                    streamTaskActionExecutor);
        }

        @Override
        public Sink.ProcessingTimeService getProcessingTimeService() {
            return new Sink.ProcessingTimeService() {
                @Override
                public long getCurrentProcessingTime() {
                    return processingTimeService.getCurrentProcessingTime();
                }

                @Override
                public void registerProcessingTimer(
                        long time, ProcessingTimeCallback processingTimerCallback) {
                    processingTimeService.registerTimer(
                            time, processingTimerCallback::onProcessingTime);
                }
            };
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

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        public TestProcessingTimeService getTestProcessingTimeService() {
            return processingTimeService;
        }
    }

    /**
     * This SinkWriter releases the lock on existing threads blocked by {@code delayedStartLatch}
     * and blocks itself until {@code blockedThreadLatch} is unblocked.
     */
    private class AsyncSinkReleaseAndBlockWriterImpl extends AsyncSinkWriterImpl {

        private final CountDownLatch blockedThreadLatch;
        private final CountDownLatch delayedStartLatch;
        private final boolean blockForLimitedTime;

        public AsyncSinkReleaseAndBlockWriterImpl(
                Sink.InitContext context,
                int maxBatchSize,
                int maxInFlightRequests,
                int maxBufferedRequests,
                long flushOnBufferSizeInBytes,
                long maxTimeInBufferMS,
                CountDownLatch blockedThreadLatch,
                CountDownLatch delayedStartLatch,
                boolean blockForLimitedTime) {
            super(
                    context,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    flushOnBufferSizeInBytes,
                    maxTimeInBufferMS,
                    false);
            this.blockedThreadLatch = blockedThreadLatch;
            this.delayedStartLatch = delayedStartLatch;
            this.blockForLimitedTime = blockForLimitedTime;
        }

        @Override
        protected void submitRequestEntries(
                List<Integer> requestEntries, Consumer<Collection<Integer>> requestResult) {
            if (requestEntries.size() == 3) {
                try {
                    delayedStartLatch.countDown();
                    if (blockForLimitedTime) {
                        assertFalse(
                                blockedThreadLatch.await(500, TimeUnit.MILLISECONDS),
                                "The countdown latch was released before the full amount"
                                        + "of time was reached.");
                    } else {
                        blockedThreadLatch.await();
                    }
                } catch (InterruptedException e) {
                    fail("The unit test latch must not have been interrupted by another thread.");
                }
            }

            res.addAll(requestEntries);
            requestResult.accept(new ArrayList<>());
        }
    }
}
