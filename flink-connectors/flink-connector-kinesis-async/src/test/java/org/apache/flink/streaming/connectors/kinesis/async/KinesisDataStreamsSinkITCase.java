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
import org.apache.flink.streaming.connectors.kinesis.async.testutils.ExampleSource;
import org.apache.flink.streaming.connectors.kinesis.async.testutils.KinesaliteContainer;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
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
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertEquals;

/** IT cases for using Kinesis Data Streams Sink based on Kinesalite. */
public class KinesisDataStreamsSinkITCase extends TestLogger {

    private static final String TEST_STREAM_NAME = "test-stream-name";
    private static final String DEFAULT_FIRST_SHARD_NAME = "shardId-000000000000";
    private static final String AWS_REGION_SYSTEM_PROP_NAME = "aws.region";

    private final ElementConverter<String, PutRecordsRequestEntry> elementConverter =
            (element, context) ->
                    PutRecordsRequestEntry.builder()
                            .data(SdkBytes.fromUtf8String(element))
                            .partitionKey(String.valueOf(element.hashCode()))
                            .build();

    @ClassRule
    public static KinesaliteContainer kinesalite =
            new KinesaliteContainer(DockerImageName.parse(DockerImageVersions.KINESALITE));

    private StreamExecutionEnvironment env;
    private KinesisAsyncClient kinesisClient;

    @Before
    public void setUp() throws Exception {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        System.setProperty(AWS_REGION_SYSTEM_PROP_NAME, kinesalite.getRegion().toString());

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        kinesisClient = kinesalite.getNewClient();
        setFinalStatic(
                KinesisDataStreamsSinkWriter.class.getDeclaredField("client"), kinesisClient);

        kinesisClient
                .createStream(
                        CreateStreamRequest.builder()
                                .streamName(TEST_STREAM_NAME)
                                .shardCount(1)
                                .build())
                .get();

        DescribeStreamResponse describeStream =
                kinesisClient
                        .describeStream(
                                DescribeStreamRequest.builder()
                                        .streamName(TEST_STREAM_NAME)
                                        .build())
                        .get();

        while (describeStream.streamDescription().streamStatus() != StreamStatus.ACTIVE) {
            describeStream =
                    kinesisClient
                            .describeStream(
                                    DescribeStreamRequest.builder()
                                            .streamName(TEST_STREAM_NAME)
                                            .build())
                            .get();
        }
    }

    @Test
    public void atest() throws Exception {

        DataStream<String> stream = env.addSource(new ExampleSource(1, 2, 1000, 969));

        KinesisDataStreamsSinkConfig.Builder<String> sinkConfigBuilder =
                KinesisDataStreamsSinkConfig.builder();
        KinesisDataStreamsSinkConfig<String> sinkConfig =
                sinkConfigBuilder
                        .setElementConverter(elementConverter)
                        .setMaxTimeInBufferMS(90)
                        .setFlushOnBufferSizeInBytes(409600)
                        .setMaxInFlightRequests(1)
                        .setMaxBatchSize(100)
                        .setMaxBufferedRequests(1000)
                        .setStreamName(TEST_STREAM_NAME)
                        .build();
        stream.sinkTo(new KinesisDataStreamsSink<>(sinkConfig));

        env.execute("KDS Async Sink Example Program");

        String shardIterator =
                kinesisClient
                        .getShardIterator(
                                GetShardIteratorRequest.builder()
                                        .shardId(DEFAULT_FIRST_SHARD_NAME)
                                        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                        .streamName(TEST_STREAM_NAME)
                                        .build())
                        .get()
                        .shardIterator();

        assertEquals(
                1,
                kinesisClient
                        .getRecords(
                                GetRecordsRequest.builder().shardIterator(shardIterator).build())
                        .get()
                        .records()
                        .size());
    }

    private static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(KinesisDataStreamsSinkWriter.class, newValue);
    }
}
