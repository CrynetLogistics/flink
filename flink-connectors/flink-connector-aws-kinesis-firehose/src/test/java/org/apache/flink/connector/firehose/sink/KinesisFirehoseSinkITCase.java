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

package org.apache.flink.connector.firehose.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.firehose.sink.testutils.LocalstackContainer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.DockerImageVersions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.firehose.model.Record;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.utils.ImmutableMap;

import java.util.List;

import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.createBucket;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.createDeliveryStream;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.createIAMRole;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.getConfig;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.getFirehoseClient;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.getIamClient;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.getS3Client;
import static org.apache.flink.connector.firehose.sink.testutils.KinesisFirehoseTestUtils.listBucketObjects;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test suite for the {@code KinesisFirehoseSink} using a localstack container. */
public class KinesisFirehoseSinkITCase {

    private static final ElementConverter<String, Record> elementConverter =
            KinesisFirehoseSinkElementConverter.<String>builder()
                    .setSerializationSchema(new SimpleStringSchema())
                    .build();

    private static final Logger LOG = LoggerFactory.getLogger(KinesisFirehoseSinkITCase.class);
    private S3AsyncClient s3AsyncClient;
    private FirehoseAsyncClient firehoseAsyncClient;
    private IamAsyncClient iamAsyncClient;

    private static final String ROLE_NAME = "super-role";
    private static final String ROLE_ARN = "arn:aws:iam::000000000000:role/" + ROLE_NAME;
    private static final String BUCKET_NAME = "s3-firehose";
    private static final String STREAM_NAME = "s3-stream";
    private static final int NUMBER_OF_ELEMENTS = 92;

    @ClassRule
    public static LocalstackContainer mockFirehoseContainer =
            new LocalstackContainer(DockerImageName.parse(DockerImageVersions.LOCALSTACK));

    @Before
    public void setup() throws Exception {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        s3AsyncClient = getS3Client(mockFirehoseContainer.getEndpoint());
        firehoseAsyncClient = getFirehoseClient(mockFirehoseContainer.getEndpoint());
        iamAsyncClient = getIamClient(mockFirehoseContainer.getEndpoint());
    }

    @After
    public void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
    }

    @Test
    public void test() throws Exception {
        LOG.info("1 - Creating the bucket for Firehose to deliver into...");
        createBucket(s3AsyncClient, BUCKET_NAME);
        LOG.info("2 - Creating the IAM Role for Firehose to write into the s3 bucket...");
        createIAMRole(iamAsyncClient, ROLE_NAME);
        LOG.info("3 - Creating the Firehose delivery stream...");
        createDeliveryStream(STREAM_NAME, BUCKET_NAME, ROLE_ARN, firehoseAsyncClient);

        ObjectMapper mapper = new ObjectMapper();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> generator =
                env.fromSequence(1, NUMBER_OF_ELEMENTS)
                        .map(Object::toString)
                        .returns(String.class)
                        .map(data -> mapper.writeValueAsString(ImmutableMap.of("data", data)));

        KinesisFirehoseSink<String> kdsSink =
                KinesisFirehoseSink.<String>builder()
                        .setElementConverter(elementConverter)
                        .setDeliveryStreamName(STREAM_NAME)
                        .setMaxBatchSize(1)
                        .setFirehoseClientProperties(getConfig(mockFirehoseContainer.getEndpoint()))
                        .build();

        generator.sinkTo(kdsSink);
        env.execute("Integration Test");

        List<S3Object> objects = listBucketObjects(s3AsyncClient, BUCKET_NAME);
        assertThat(objects.size()).isEqualTo(NUMBER_OF_ELEMENTS);
    }
}
