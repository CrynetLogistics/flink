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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connectors.kinesis.testutils.KinesaliteContainer;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.rnorth.ducttape.ratelimits.RateLimiter;
import org.rnorth.ducttape.ratelimits.RateLimiterBuilder;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.time.Duration;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;

/** IT cases for using Kinesis Data Streams Sink based on Kinesalite. */
public class KinesisStreamsSinkITCase2 extends TestLogger {

    private static final String DEFAULT_FIRST_SHARD_NAME = "shardId-000000000000";

    private final SerializationSchema<String> serializationSchema = new SimpleStringSchema();
    private final PartitionKeyGenerator<String> partitionKeyGenerator =
            element -> String.valueOf(element.hashCode());
    private final PartitionKeyGenerator<String> longPartitionKeyGenerator = element -> element;

    @ClassRule
    public static final KinesaliteContainer KINESALITE =
            new KinesaliteContainer(DockerImageName.parse(DockerImageVersions.KINESALITE))
                    .withNetwork(Network.newNetwork())
                    .withNetworkAliases("kinesalite");

    private StreamExecutionEnvironment env;
    private SdkAsyncHttpClient httpClient;
    private KinesisAsyncClient kinesisClient;

    @Before
    public void setUp() throws Exception {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        httpClient = KINESALITE.buildSdkAsyncHttpClient();
        kinesisClient = KINESALITE.createHostClient(httpClient);
    }

    @After
    public void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
        AWSGeneralUtil.closeResources(httpClient, kinesisClient);
    }

    private Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.setProperty(AWS_ENDPOINT, KINESALITE.getHostEndpointUrl());
        properties.setProperty(AWS_ACCESS_KEY_ID, KINESALITE.getAccessKey());
        properties.setProperty(AWS_SECRET_ACCESS_KEY, KINESALITE.getSecretKey());
        properties.setProperty(AWS_REGION, KINESALITE.getRegion().toString());
        return properties;
    }

    @Test
    public void runScenario() throws Exception {
        int numberOfElementsToSend = 50;
        int sizeOfMessageBytes = 25;
        int bufferMaxTimeMS = 1000;
        int maxInflightReqs = 1;
        int maxBatchSize = 50;
        int expectedElements = 50;
        boolean failOnError = false;
        String kinesaliteStreamName = "default-stream-1";
        String sinkConnectionStreamName = "default-stream-1";
        SerializationSchema<String> serializationSchema = KinesisStreamsSinkITCase2.this.serializationSchema;
        PartitionKeyGenerator<String> partitionKeyGenerator = KinesisStreamsSinkITCase2.this.partitionKeyGenerator;
        Properties properties = KinesisStreamsSinkITCase2.this.getDefaultProperties();

        if (kinesaliteStreamName != null) {
            prepareStream(kinesaliteStreamName);
        }

        properties.setProperty(TRUST_ALL_CERTIFICATES, "true");
        properties.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");

        DataStream<String> stream =
                env.addSource(
                                new DataGeneratorSource<String>(
                                        RandomGenerator.stringGenerator(sizeOfMessageBytes),
                                        100,
                                        (long) numberOfElementsToSend))
                        .returns(String.class);

        KinesisStreamsSink<String> kdsSink =
                KinesisStreamsSink.<String>builder()
                        .setSerializationSchema(serializationSchema)
                        .setPartitionKeyGenerator(partitionKeyGenerator)
                        .setMaxTimeInBufferMS(bufferMaxTimeMS)
                        .setMaxInFlightRequests(maxInflightReqs)
                        .setMaxBatchSize(maxBatchSize)
                        .setFailOnError(failOnError)
                        .setMaxBufferedRequests(1000)
                        .setStreamName(sinkConnectionStreamName)
                        .setKinesisClientProperties(properties)
                        .setFailOnError(true)
                        .build();

        stream.sinkTo(kdsSink);

        env.executeAsync("KDS Async Sink Example Program");

        String shardIterator =
                kinesisClient
                        .getShardIterator(
                                GetShardIteratorRequest.builder()
                                        .shardId(DEFAULT_FIRST_SHARD_NAME)
                                        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                        .streamName(kinesaliteStreamName)
                                        .build())
                        .get()
                        .shardIterator();

        int streamRecords = kinesisClient
                .getRecords(
                        GetRecordsRequest.builder()
                                .shardIterator(shardIterator)
                                .build())
                .get()
                .records()
                .size();
        System.out.println(streamRecords);
        System.out.println(expectedElements);
        Assertions.assertThat(streamRecords).isEqualTo(expectedElements);
    }

    private void prepareStream(String streamName) throws Exception {
        final RateLimiter rateLimiter =
                RateLimiterBuilder.newBuilder()
                        .withRate(1, SECONDS)
                        .withConstantThroughput()
                        .build();

        kinesisClient
                .createStream(
                        CreateStreamRequest.builder()
                                .streamName(streamName)
                                .shardCount(1)
                                .build())
                .get();

        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(1));
        while (!rateLimiter.getWhenReady(() -> streamExists(streamName))) {
            if (deadline.isOverdue()) {
                throw new RuntimeException("Failed to create stream within time");
            }
        }
    }

    private boolean streamExists(final String streamName) {
        try {
            return kinesisClient
                    .describeStream(
                            DescribeStreamRequest.builder()
                                    .streamName(streamName)
                                    .build())
                    .get()
                    .streamDescription()
                    .streamStatus()
                    == StreamStatus.ACTIVE;
        } catch (Exception e) {
            return false;
        }
    }
}
