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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.ListStreamsResult;

import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.utils.AttributeMap;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_ENV_VAR;

/**
 * A testcontainer based on Kinesalite.
 *
 * <p>Note that the more obvious localstack container with Kinesis took 1 minute to start vs 10
 * seconds of Kinesalite.
 */
public class KinesaliteContainer extends GenericContainer<KinesaliteContainer> {
    static {
        //for localhost testing only
        javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
                new javax.net.ssl.HostnameVerifier(){

                    public boolean verify(String hostname,
                                          javax.net.ssl.SSLSession sslSession) {
                        return hostname.equals("localhost");
                    }
                });
    }

    private SdkAsyncHttpClient buildSdkAsyncHttpClient() {
        return NettyNioAsyncHttpClient.builder()
                .buildWithDefaults(
                        AttributeMap.builder()
                                .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                                .build()
                );
    }

    private static final String ACCESS_KEY = "access key";
    private static final String SECRET_KEY = "secret key";

    public KinesaliteContainer(DockerImageName imageName) {
        super(imageName);

        withEnv(ACCESS_KEY_ENV_VAR, ACCESS_KEY);
        withEnv(SECRET_KEY_ENV_VAR, SECRET_KEY); // todo: secret key?
        withExposedPorts(4567);
        withExposedPorts(4567);
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

    /** Returns the endpoint url to access the container from outside the docker network. */
    public String getContainerEndpointUrl() {
        return String.format("https://%s:%s", getContainerIpAddress(), getMappedPort(4567));
        //return String.format("http://%s:%s", getContainerIpAddress(), getMappedPort(4567));
    }

    /** Returns the endpoint url to access the host from inside the docker network. */
    public String getHostEndpointUrl() {
        return String.format("https://%s:%s", getHost(), getMappedPort(4567));
    }

    public String getAccessKey() {
        return ACCESS_KEY;
    }

    public String getSecretKey() {
        return SECRET_KEY;
    }

//    /** Returns the properties to access the container from outside the docker network. */
//    public Properties getContainerProperties() {
//        return getProperties(getContainerEndpointUrl());
//    }
//
//    /** Returns the properties to access the host from inside the docker network. */
//    public Properties getHostProperties() {
//        return getProperties(getHostEndpointUrl());
//    }

    /** Returns the client to access the container from outside the docker network. */
    public AmazonKinesis getContainerClient() {
        return getClient(getContainerEndpointUrl());
    }

    /** Returns the client to access the host from inside the docker network. */
    public AmazonKinesis getHostClient() {
        return getClient(getHostEndpointUrl());
    }

    private AmazonKinesis getClient(String endPoint) {
        return AmazonKinesisClientBuilder.standard()
                .withCredentials(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(getAccessKey(), getSecretKey())))
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(endPoint, "us-east-1"))
                .build();
    }

    public KinesisAsyncClient getNewClient() throws URISyntaxException {
        System.out.println(getContainerEndpointUrl());
        System.out.println(getHostEndpointUrl());
        return KinesisAsyncClient
                .builder()
                .endpointOverride(new URI(getHostEndpointUrl()))
                .region(Region.US_EAST_1)
                .credentialsProvider(() -> AwsBasicCredentials.create(getAccessKey(), getSecretKey()))
                .httpClient(buildSdkAsyncHttpClient())
                .build();
    }

//    private Properties getProperties(String endpointUrl) {
//        Properties config = new Properties();
//        config.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
//        config.setProperty(AWSConfigConstants.AWS_ENDPOINT, endpointUrl);
//        config.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, getAccessKey());
//        config.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, getSecretKey());
//        return config;
//    }

    private class ListStreamsWaitStrategy extends AbstractWaitStrategy {
        @Override
        protected void waitUntilReady() {
            Unreliables.retryUntilSuccess(
                    (int) this.startupTimeout.getSeconds(),
                    TimeUnit.SECONDS,
                    () -> this.getRateLimiter().getWhenReady(() -> tryList()));
        }

        private ListStreamsResult tryList() {
            return getContainerClient().listStreams();
        }
    }
}
