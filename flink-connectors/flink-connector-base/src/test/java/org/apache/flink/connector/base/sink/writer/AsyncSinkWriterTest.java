package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.Sink;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class AsyncSinkWriterTest {

    List<Integer> res = new ArrayList<>();

    @Test
    public void test() throws IOException, InterruptedException {
        Context context = new Context();
        AsyncSinkWriterImpl sink = new AsyncSinkWriterImpl(context);

        for(int i=0; i<101; i++){
            sink.write(String.valueOf(i), null);
        }

        System.out.println(res.size());
    }

    private class AsyncSinkWriterImpl extends AsyncSinkWriter<String, Integer> {

        public AsyncSinkWriterImpl(Sink.InitContext context) {
            super((elem, ctx) -> Integer.parseInt(elem), context);
        }

        @Override
        protected void submitRequestEntries(
                List<Integer> requestEntries,
                ResultFuture<Integer> requestResult) {
            res.addAll(requestEntries);
            requestResult.complete(new ArrayList<>());
        }
    }

    private class Context implements Sink.InitContext {

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
            return new MailboxExecutorImpl(new TaskMailboxImpl(Thread.currentThread()), 10, streamTaskActionExecutor);
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
