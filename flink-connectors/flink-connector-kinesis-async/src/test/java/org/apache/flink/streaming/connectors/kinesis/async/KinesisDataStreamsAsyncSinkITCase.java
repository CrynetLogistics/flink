package org.apache.flink.streaming.connectors.kinesis.async;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import org.junit.jupiter.api.Test;

import java.util.Properties;

/** a. */
public class KinesisDataStreamsAsyncSinkITCase {

    @Test
    public void test() throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10_000);

        //        DataStream<String> stream =
        // env.readTextFile("s3://shausma-nyc-tlc/yellow-trip-data/taxi-trips.json/dropoff_year=2010/part-00000-cdac5fe4-b823-4576-aeb7-7327b077476e.c000.json");

        Properties consumerConfig = new Properties();
        consumerConfig.put("aws.region", "eu-west-1");
        consumerConfig.put("aws.credentials.provider", "AUTO");
        consumerConfig.put("flink.stream.initpos", "TRIM_HORIZON");

        DataStream<String> fromGen =
                env.addSource(
                        new RichSourceFunction<String>() {
                            private static final long serialVersionUID = 1L;
                            private volatile boolean running = true;
                            private int emittedCount = 0;

                            public void run(SourceContext<String> ctx) throws Exception {
                                for (; this.running; Thread.sleep(5L)) {
                                    synchronized (ctx.getCheckpointLock()) {
                                        ctx.collect("{\"time\":" + this.emittedCount + ",\"woo\":45}");
                                    }

                                    if (this.emittedCount < 1000) {
                                        ++this.emittedCount;
                                    } else {
                                        this.emittedCount = 0;
                                    }
                                }
                            }

                            public void cancel() {
                                this.running = false;
                            }
                        });

        fromGen.map(
                        x -> {
                            System.out.println(x);
                            return x;
                        })
                .sinkTo(new KinesisDataStreamsAsyncSink<>());

        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataStream<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * https://flink.apache.org/docs/latest/apis/streaming/index.html
         *
         */

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
