package org.apache.flink.connector.aws.testutils;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/** A source that does not shut down until {@code finish()} is called. */
public class DelayedShutdownSource extends RichParallelSourceFunction<Integer> {

    private volatile boolean cancelled = false;
    private static final CountDownLatch latch = new CountDownLatch(1);
    private static final AtomicInteger count = new AtomicInteger(110);

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        System.out.println("Trying to collect an element from Source.......");
        while (!cancelled) {
            int currentCount = count.getAndDecrement();
            if (currentCount > 100) {
                ctx.collect(currentCount);
            } else {
                System.out.println("No more elements to collect from Source.........");
                latch.await();
                System.out.println("Received signal to shut down source");
                ctx.emitWatermark(Watermark.MAX_WATERMARK);
                return;
            }
        }
    }

    public void finish() {
        latch.countDown();
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}
