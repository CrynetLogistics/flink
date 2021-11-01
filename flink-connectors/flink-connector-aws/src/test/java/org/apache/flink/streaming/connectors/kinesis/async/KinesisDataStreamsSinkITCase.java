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
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
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

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.connectors.kinesis.async.util.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.streaming.connectors.kinesis.async.util.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.streaming.connectors.kinesis.async.util.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.streaming.connectors.kinesis.async.util.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.junit.Assert.assertEquals;

/** IT cases for using Kinesis Data Streams Sink based on Kinesalite. */
public class KinesisDataStreamsSinkITCase extends TestLogger {

    private static final String DEFAULT_FIRST_SHARD_NAME = "shardId-000000000000";

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

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        kinesisClient = kinesalite.getV2Client();
    }

    @Test
    public void elementsMaybeWrittenSuccessfullyToLocalInstanceWhenBatchSizeIsReached()
            throws Exception {
        runScenario(50, 25, 1000, 819200, 1, 50, 50, "test-stream-name-1");
    }

    @Test
    public void elementsBufferedAndTriggeredByTimeBasedFlushShouldBeFlushedIfSourcedIsKeptAlive()
            throws Exception {
        int timeBasedFlushingThresholdMS = 1000;
        runScenario(10, 25, timeBasedFlushingThresholdMS, 819200, 1, 100, 10, "test-stream-name-2");
    }

    @Test
    public void veryLargeMessagesSucceedInBeingPersisted() throws Exception {
        runScenario(5, 2500, 1000, 8192, 1, 10, 5, "test-stream-name-3");
    }

    @Test
    public void multipleInFlightRequestsResultsInCorrectNumberOfElementsPersisted()
            throws Exception {
        runScenario(150, 2500, 1000, 8192, 10, 20, 150, "test-stream-name-4");
    }

    private void runScenario(
            int numberOfElementsToSend,
            int sizeOfMessageBytes,
            int bufferMaxTimeMS,
            int bufferMaxSizeBytes,
            int maxInflightReqs,
            int maxBatchSize,
            int expectedElements,
            String testStreamName)
            throws Exception {

        prepareStream(testStreamName);

        DataStream<String> stream =
                env.addSource(
                                new DataGeneratorSource<String>(
                                        RandomGenerator.stringGenerator(sizeOfMessageBytes),
                                        100,
                                        (long) numberOfElementsToSend))
                        .returns(String.class);

        Properties prop = new Properties();
        prop.setProperty(AWS_ENDPOINT, kinesalite.getHostEndpointUrl());
        prop.setProperty(AWS_ACCESS_KEY_ID, kinesalite.getAccessKey());
        prop.setProperty(AWS_SECRET_ACCESS_KEY, kinesalite.getSecretKey());
        prop.setProperty(AWS_REGION, kinesalite.getRegion().toString());
        prop.setProperty("TRUST_ALL_CERTIFICATES", "true");

        KinesisDataStreamsSink<String> kdsSink =
                KinesisDataStreamsSink.<String>builder()
                        .setElementConverter(elementConverter)
                        .setMaxTimeInBufferMS(bufferMaxTimeMS)
                        .setFlushOnBufferSizeInBytes(bufferMaxSizeBytes)
                        .setMaxInFlightRequests(maxInflightReqs)
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBufferedRequests(1000)
                        .setStreamName(testStreamName)
                        .setKinesisClientProperties(prop)
                        .build();

        stream.sinkTo(kdsSink);

        env.execute("KDS Async Sink Example Program");

        String shardIterator =
                kinesisClient
                        .getShardIterator(
                                GetShardIteratorRequest.builder()
                                        .shardId(DEFAULT_FIRST_SHARD_NAME)
                                        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                        .streamName(testStreamName)
                                        .build())
                        .get()
                        .shardIterator();

        assertEquals(
                expectedElements,
                kinesisClient
                        .getRecords(
                                GetRecordsRequest.builder().shardIterator(shardIterator).build())
                        .get()
                        .records()
                        .size());
    }

    private void prepareStream(String testStreamName)
            throws InterruptedException, ExecutionException {
        kinesisClient
                .createStream(
                        CreateStreamRequest.builder()
                                .streamName(testStreamName)
                                .shardCount(1)
                                .build())
                .get();

        DescribeStreamResponse describeStream =
                kinesisClient
                        .describeStream(
                                DescribeStreamRequest.builder().streamName(testStreamName).build())
                        .get();

        while (describeStream.streamDescription().streamStatus() != StreamStatus.ACTIVE) {
            describeStream =
                    kinesisClient
                            .describeStream(
                                    DescribeStreamRequest.builder()
                                            .streamName(testStreamName)
                                            .build())
                            .get();
        }
    }
}
