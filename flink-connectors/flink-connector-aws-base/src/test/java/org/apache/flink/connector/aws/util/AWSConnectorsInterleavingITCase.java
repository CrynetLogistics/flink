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

package org.apache.flink.connector.aws.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.testutils.DelayedShutdownSource;
import org.apache.flink.connector.aws.testutils.DelayedStartSource;
import org.apache.flink.connector.aws.testutils.IamSink;
import org.apache.flink.connector.aws.testutils.LocalstackContainer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.DockerImageVersions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.ListRolesRequest;
import software.amazon.awssdk.services.iam.model.ListRolesResponse;
import software.amazon.awssdk.services.iam.model.Role;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createIamClient;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test suite for the {@code KinesisFirehoseSink} using a localstack container. */
@Testcontainers
class AWSConnectorsInterleavingITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(AWSConnectorsInterleavingITCase.class);
    private StreamExecutionEnvironment env1;
    private StreamExecutionEnvironment env2;

    @Container
    private static final LocalstackContainer mockFirehoseContainer =
            new LocalstackContainer(DockerImageName.parse(DockerImageVersions.LOCALSTACK));

    @BeforeEach
    void setup() {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
        env1 = StreamExecutionEnvironment.createLocalEnvironment();
        env2 = StreamExecutionEnvironment.createLocalEnvironment();
    }

    @AfterEach
    void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
    }

    @Test
    void firehoseSinkWritesCorrectDataToMockAWSServices() throws Exception {

        IamSink<String> kdsSink1 = new IamSink<>(mockFirehoseContainer.getEndpoint());
        IamSink<String> kdsSink2 = new IamSink<>(mockFirehoseContainer.getEndpoint());

        DelayedShutdownSource source = new DelayedShutdownSource();
        DelayedStartSource source2 = new DelayedStartSource();

        env1.addSource(source).map(Object::toString).returns(String.class).sinkTo(kdsSink1);

        env2.addSource(source2).map(Object::toString).returns(String.class).sinkTo(kdsSink2);

        LOG.info("Starting job 1");
        JobClient jobClient1 = env1.executeAsync("Branch 1");
        LOG.info("Sleeping for 1000ms, then starting job 2");
        Thread.sleep(1000);
        JobClient jobClient2 = env2.executeAsync("Branch 2");

        LOG.info("Finishing job 1");
        source.finish();
        JobExecutionResult res1 = jobClient1.getJobExecutionResult().join();
        System.out.println(res1);

        LOG.info("Finishing job 2");
        source2.start();
        JobExecutionResult res2 = jobClient2.getJobExecutionResult().join();
        System.out.println(res2);

        SdkAsyncHttpClient httpClient =
                AWSServicesTestUtils.createHttpClient(mockFirehoseContainer.getEndpoint());
        IamAsyncClient iamAsyncClient =
                createIamClient(mockFirehoseContainer.getEndpoint(), httpClient);

        List<String> expectedList =
                IntStream.range(1, 21).mapToObj(String::valueOf).collect(Collectors.toList());
        expectedList.addAll(
                IntStream.range(101, 111).mapToObj(String::valueOf).collect(Collectors.toList()));
        ListRolesRequest listAllRolesRequest = ListRolesRequest.builder().build();
        ListRolesResponse res = iamAsyncClient.listRoles(listAllRolesRequest).get();
        List<String> retrievedRoleNames =
                res.roles().stream().map(Role::roleName).collect(Collectors.toList());
        assertThat(retrievedRoleNames).containsAll(expectedList);

        Set<String> classloaderNames = AWSGeneralUtil.getRegisteredClassloaderNames();
        assertThat(classloaderNames.size()).isEqualTo(2);
    }
}
