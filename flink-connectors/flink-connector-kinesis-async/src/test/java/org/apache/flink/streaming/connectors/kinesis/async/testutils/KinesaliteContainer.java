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

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_ENV_VAR;

/**
 * A {@code org.testcontainers} based on Kinesalite.
 *
 * <p>Note that the more obvious localstack container with Kinesis took 1 minute to start vs 10
 * seconds of Kinesalite.
 */
public class KinesaliteContainer extends GenericContainer<KinesaliteContainer> {

    private static final int KINESALITE_PORT = 4567;
    private static final String ACCESS_KEY = "access key";
    private static final String SECRET_KEY = "secret key";
    private static final Region REGION = Region.US_EAST_1;

    public KinesaliteContainer(DockerImageName imageName) {
        super(imageName);

        withEnv(ACCESS_KEY_ENV_VAR, ACCESS_KEY);
        withEnv(SECRET_KEY_ENV_VAR, SECRET_KEY);
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
