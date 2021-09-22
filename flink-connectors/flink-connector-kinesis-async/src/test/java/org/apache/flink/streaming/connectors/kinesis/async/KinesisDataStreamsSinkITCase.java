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

package org.apache.flink.streaming.connectors.kinesis.async;

import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kinesis.async.testutils.KinesaliteContainer;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** IT cases for using Kinesis consumer/producer based on Kinesalite. */
public class KinesisDataStreamsSinkITCase extends TestLogger {

    private final ElementConverter<String, PutRecordsRequestEntry> elementConverter =
            (element, context) ->
                    PutRecordsRequestEntry.builder()
                            .data(SdkBytes.fromUtf8String(element))
                            .partitionKey(String.valueOf(element.hashCode()))
                            .build();

    @ClassRule
    public static KinesaliteContainer kinesalite =
            new KinesaliteContainer(DockerImageName.parse(DockerImageVersions.KINESALITE));

    @Test
    public void testStopWithSavepoint() throws Exception {

        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        System.setProperty("aws.region", "us-east-1");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KinesisAsyncClient kiness = kinesalite.getNewClient();
        setFinalStatic(KinesisDataStreamsSinkWriter.class.getDeclaredField("client"), kiness);
        kiness.createStream(CreateStreamRequest.builder().streamName("py-output").shardCount(1).build()).get();
        DescribeStreamResponse res = kiness.describeStream(DescribeStreamRequest.builder().streamName("py-output").build()).get();
        System.out.println(res);
        while(res.streamDescription().streamStatus() != StreamStatus.ACTIVE){
            res = kiness.describeStream(DescribeStreamRequest.builder().streamName("py-output").build()).get();
        }
        System.out.println(res);


        DataStream<String> stream = env.addSource(new ExampleSource());



        KinesisDataStreamsSink.Builder<String> kdsSinkBuilder = KinesisDataStreamsSink.builder();
        KinesisDataStreamsSink<String> kdsSink =
                kdsSinkBuilder
                        .setElementConverter(elementConverter)
                        .setMaxTimeInBufferMS(10000)
                        .setFlushOnBufferSizeInBytes(409600)
                        .setMaxInFlightRequests(1)
                        .setMaxBatchSize(100)
                        .setMaxBufferedRequests(1000)
                        .build();
        stream.sinkTo(kdsSink);
        env.execute("KDS Async Sink Example Program");

        System.out.println(kiness.listShards(ListShardsRequest.builder().streamName("py-output").build()).get().shards().stream().map(x -> x.toString()).collect(
                Collectors.toList()));

        Thread.sleep(3000);

        String shardIterator = kiness.getShardIterator(GetShardIteratorRequest.builder().shardId("shardId-000000000000").shardIteratorType(
                ShardIteratorType.TRIM_HORIZON).streamName("py-output").build()).get().shardIterator();

        assertEquals(100,
                kiness.getRecords(
                        GetRecordsRequest.builder().shardIterator(shardIterator).build()).get().records().size());
    }

    static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(KinesisDataStreamsSinkWriter.class, newValue);
    }

    public static class ExampleSource extends RichSourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;
        private int emittedCount = 0;

        public void run(SourceContext<String> ctx) throws Exception {
            for (; this.running; Thread.sleep(5L)) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(
                            "{\"time\":" + this.emittedCount + ",\"woo\":45}");
                }

                if (this.emittedCount < 100) {
                    ++this.emittedCount;
                } else {
                    return;
                }
            }
        }

        public void cancel() {
            this.running = false;
        }
    }
}
