package org.apache.flink.connector.aws.testutils;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/** A source that does not begin writing until {@code start()} is called. */
public class DelayedStartSource extends RichParallelSourceFunction<Integer> {

    private volatile boolean cancelled = false;
    private static final CountDownLatch latch = new CountDownLatch(1);
    private static final AtomicInteger count = new AtomicInteger(20);

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (!cancelled) {
            latch.await();
            System.out.println("Job 2 now beginning to write to KDF");
            int currentCount = count.getAndDecrement();
            if (currentCount > 0) {
                ctx.collect(currentCount);
            } else {
                ctx.emitWatermark(Watermark.MAX_WATERMARK);
                return;
            }
        }
    }

    public void start() {
        latch.countDown();
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}
