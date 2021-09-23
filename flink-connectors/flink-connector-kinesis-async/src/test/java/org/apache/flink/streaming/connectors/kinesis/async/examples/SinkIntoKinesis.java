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
package org.apache.flink.streaming.connectors.kinesis.async.examples;

import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kinesis.async.KinesisDataStreamsSink;
import org.apache.flink.streaming.connectors.kinesis.async.KinesisDataStreamsSinkConfig;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

/**
 * An example application on how to sink into KDS.
 *
 * <p>The {@link KinesisAsyncClient} used here may be configured in the standard way for the AWS SDK
 * 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and {@code
 * AWS_SECRET_ACCESS_KEY} through environment variables etc.
 */
public class SinkIntoKinesis {

    private static final ElementConverter<String, PutRecordsRequestEntry> elementConverter =
            (element, context) ->
                    PutRecordsRequestEntry.builder()
                            .data(SdkBytes.fromUtf8String(element))
                            .partitionKey(String.valueOf(element.hashCode()))
                            .build();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<String> fromGen = env.addSource(new ExampleDataSourceFunction());

        KinesisDataStreamsSinkConfig.Builder<String> kdsSinkBuilder =
                KinesisDataStreamsSinkConfig.builder();
        KinesisDataStreamsSinkConfig<String> kdsSink =
                kdsSinkBuilder
                        .setElementConverter(elementConverter)
                        .setStreamName("your_stream_name")
                        .build();

        fromGen.sinkTo(new KinesisDataStreamsSink<>(kdsSink));

        env.execute("KDS Async Sink Example Program");
    }

    private static class ExampleDataSourceFunction extends RichSourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;
        private int emittedCount = 1000000000;

        public void run(SourceContext<String> ctx) throws Exception {
            for (; this.running; Thread.sleep(5L)) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(
                            "{\"isin\":\"US"
                                    + this.emittedCount
                                    + "\",\"price\":"
                                    + generateBondPrice()
                                    + "}");
                }

                if (this.emittedCount < Integer.MAX_VALUE) {
                    ++this.emittedCount;
                } else {
                    this.emittedCount = 1000000000;
                }
            }
        }

        private String generateBondPrice() {
            return String.valueOf(100 + 200 * (Math.random() - .5));
        }

        public void cancel() {
            this.running = false;
        }
    }
}
