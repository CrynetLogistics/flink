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

package org.apache.flink.streaming.connectors.kinesis.async.testutils;

import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.utils.AttributeMap;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A {@code org.testcontainers} based on Kinesalite.
 *
 * <p>Note that the more obvious localstack container with Kinesis took 1 minute to start vs 3
 * seconds of Kinesalite.
 *
 * <p>TODO: THIS IMPLEMENTATION OCCASIONALLY FAILS ON THE BUILD SERVER. ***STOP*** FIX THIS BEFORE
 * MERGING. THIS IMPLEMENTATION IS BASED ON THE KINESIS TEST CONTAINER AT {@code
 * org.apache.flink.streaming.connectors.kinesis.testutils.KinesaliteContainer1} WHICH IS ALSO
 * BROKEN IN THE SAME WAY. TESTS USING THAT CONTAINER ARE CURRENTLY BEING IGNORED. THE FLINK TASK
 * ASSIGNED TO THAT @IGNORE DOES NOT ADDRESS THIS ISSUE, BUT SOME OTHER ISSUE.
 */
public class KinesaliteContainer1 extends GenericContainer<KinesaliteContainer1> {

    private static final int KINESALITE_PORT = 4567;
    private static final String ACCESS_KEY = "access key";
    private static final String SECRET_KEY = "secret key";
    private static final Region REGION = Region.US_EAST_1;
    private static final String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
    private static final String AWS_SECRET_KEY = "AWS_SECRET_KEY";

    public KinesaliteContainer1(DockerImageName imageName) {
        super(imageName);

        withEnv(AWS_ACCESS_KEY_ID, ACCESS_KEY);
        withEnv(AWS_SECRET_KEY, SECRET_KEY);
        withExposedPorts(KINESALITE_PORT);
        waitingFor(new ListStreamsWaitStrategy());
        withCreateContainerCmdModifier(
                cmd ->
                        cmd.withEntrypoint(
                                "/tini",
                                "--",
                                "/usr/src/app/node_modules/kinesalite/cli.js",
                                "--path",
                                "/var/lib/kinesalite",
                                "--ssl"));
    }

    public String getHostEndpointUrl() {
        return String.format("https://%s:%s", getHost(), getMappedPort(KINESALITE_PORT));
    }

    public String getAccessKey() {
        return ACCESS_KEY;
    }

    public String getSecretKey() {
        return SECRET_KEY;
    }

    public Region getRegion() {
        return REGION;
    }

    public KinesisAsyncClient getNewClient() throws URISyntaxException {
        return KinesisAsyncClient.builder()
                .endpointOverride(new URI(getHostEndpointUrl()))
                .region(REGION)
                .credentialsProvider(
                        () -> AwsBasicCredentials.create(getAccessKey(), getSecretKey()))
                .httpClient(buildSdkAsyncHttpClient())
                .build();
    }

    private class ListStreamsWaitStrategy extends AbstractWaitStrategy {
        @Override
        protected void waitUntilReady() {
            Unreliables.retryUntilSuccess(
                    (int) this.startupTimeout.getSeconds(),
                    TimeUnit.SECONDS,
                    () -> this.getRateLimiter().getWhenReady(this::tryList));
        }

        private ListStreamsResponse tryList()
                throws URISyntaxException, ExecutionException, InterruptedException {
            return getNewClient().listStreams().get();
        }
    }

    private SdkAsyncHttpClient buildSdkAsyncHttpClient() {
        return NettyNioAsyncHttpClient.builder()
                .buildWithDefaults(
                        AttributeMap.builder()
                                .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                                .build());
    }
}
