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

package org.apache.flink.connector.kinesis.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.utils.AttributeMap;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.connector.kinesis.config.AWSKinesisDataStreamsConfigConstants.DEFAULT_HTTP_CLIENT_MAX_CONCURRENCY;
import static org.apache.flink.connector.kinesis.config.AWSKinesisDataStreamsConfigConstants.DEFAULT_HTTP_CLIENT_READ_TIMEOUT;
import static org.apache.flink.connector.kinesis.config.AWSKinesisDataStreamsConfigConstants.DEFAULT_HTTP_PROTOCOL;
import static org.apache.flink.connector.kinesis.config.AWSKinesisDataStreamsConfigConstants.DEFAULT_LEGACY_CONNECTOR;
import static org.apache.flink.connector.kinesis.config.AWSKinesisDataStreamsConfigConstants.DEFAULT_TRUST_ALL_CERTIFICATES;

/** Some utilities specific to Amazon Web Service. */
@Internal
public class AWSKinesisDataStreamsUtil extends AWSGeneralUtil {
    /** Used for formatting Flink-specific user agent string when creating Kinesis client. */
    private static final String LEGACY_USER_AGENT_FORMAT = "Apache Flink %s (%s) Kinesis Connector";

    private static final String USER_AGENT_FORMAT = LEGACY_USER_AGENT_FORMAT + " V2";

    private static final int INITIAL_WINDOW_SIZE_BYTES = 512 * 1024; // 512 KB
    private static final Duration HEALTH_CHECK_PING_PERIOD = Duration.ofSeconds(60);

    @VisibleForTesting
    static final Duration CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(60);

    private static final AttributeMap HTTP_CLIENT_DEFAULTS =
            AttributeMap.builder()
                    .put(
                            SdkHttpConfigurationOption.MAX_CONNECTIONS,
                            DEFAULT_HTTP_CLIENT_MAX_CONCURRENCY)
                    .put(SdkHttpConfigurationOption.READ_TIMEOUT, DEFAULT_HTTP_CLIENT_READ_TIMEOUT)
                    .put(
                            SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES,
                            DEFAULT_TRUST_ALL_CERTIFICATES)
                    .put(SdkHttpConfigurationOption.PROTOCOL, DEFAULT_HTTP_PROTOCOL)
                    .build();

    /**
     * Creates a user agent prefix for Flink. This can be used by HTTP Clients.
     *
     * @param isLegacyConnector flag to use appropriate user agent for the connector
     * @return a user agent prefix for Flink
     */
    public static String formatFlinkUserAgentPrefix(boolean isLegacyConnector) {
        return String.format(
                isLegacyConnector ? LEGACY_USER_AGENT_FORMAT : USER_AGENT_FORMAT,
                EnvironmentInformation.getVersion(),
                EnvironmentInformation.getRevisionInformation().commitId);
    }

    public static SdkAsyncHttpClient createHttpClient(
            final NettyNioAsyncHttpClient.Builder httpClientBuilder) {
        return createHttpClient(AttributeMap.empty(), httpClientBuilder);
    }

    public static SdkAsyncHttpClient createHttpClient(
            final AttributeMap config, final NettyNioAsyncHttpClient.Builder httpClientBuilder) {
        httpClientBuilder
                .connectionAcquisitionTimeout(CONNECTION_ACQUISITION_TIMEOUT)
                .http2Configuration(
                        Http2Configuration.builder()
                                .healthCheckPingPeriod(HEALTH_CHECK_PING_PERIOD)
                                .initialWindowSize(INITIAL_WINDOW_SIZE_BYTES)
                                .build());
        return AWSGeneralUtil.createHttpClient(
                config.merge(HTTP_CLIENT_DEFAULTS), httpClientBuilder);
    }

    /**
     * @param configProps configuration properties
     * @param httpClient the underlying HTTP client used to talk to Kinesis
     * @return a new Amazon Kinesis Client
     */
    public static KinesisAsyncClient createKinesisAsyncClient(
            final Properties configProps, final SdkAsyncHttpClient httpClient) {
        SdkClientConfiguration clientConfiguration = SdkClientConfiguration.builder().build();
        return createKinesisAsyncClient(configProps, clientConfiguration, httpClient);
    }

    /**
     * @param configProps configuration properties
     * @param clientConfiguration the AWS SDK v2 config to instantiate the client
     * @param httpClient the underlying HTTP client used to talk to Kinesis
     * @return a new Amazon Kinesis Client
     */
    public static KinesisAsyncClient createKinesisAsyncClient(
            final Properties configProps,
            final SdkClientConfiguration clientConfiguration,
            final SdkAsyncHttpClient httpClient) {
        boolean isLegacyConnector =
                Optional.ofNullable(configProps.getProperty(AWSConfigConstants.LEGACY_CONNECTOR))
                        .map(Boolean::parseBoolean)
                        .orElse(DEFAULT_LEGACY_CONNECTOR);

        final ClientOverrideConfiguration overrideConfiguration =
                createClientOverrideConfiguration(
                        clientConfiguration,
                        ClientOverrideConfiguration.builder(),
                        isLegacyConnector);
        final KinesisAsyncClientBuilder clientBuilder = KinesisAsyncClient.builder();

        return createKinesisAsyncClient(
                configProps, clientBuilder, httpClient, overrideConfiguration);
    }

    @VisibleForTesting
    static ClientOverrideConfiguration createClientOverrideConfiguration(
            final SdkClientConfiguration config,
            final ClientOverrideConfiguration.Builder overrideConfigurationBuilder,
            boolean isLegacyConnector) {

        overrideConfigurationBuilder
                .putAdvancedOption(
                        SdkAdvancedClientOption.USER_AGENT_PREFIX,
                        formatFlinkUserAgentPrefix(isLegacyConnector))
                .putAdvancedOption(
                        SdkAdvancedClientOption.USER_AGENT_SUFFIX,
                        config.option(SdkAdvancedClientOption.USER_AGENT_SUFFIX));

        Optional.ofNullable(config.option(SdkClientOption.API_CALL_ATTEMPT_TIMEOUT))
                .ifPresent(overrideConfigurationBuilder::apiCallAttemptTimeout);

        Optional.ofNullable(config.option(SdkClientOption.API_CALL_TIMEOUT))
                .ifPresent(overrideConfigurationBuilder::apiCallTimeout);

        return overrideConfigurationBuilder.build();
    }

    @VisibleForTesting
    static KinesisAsyncClient createKinesisAsyncClient(
            final Properties configProps,
            final KinesisAsyncClientBuilder clientBuilder,
            final SdkAsyncHttpClient httpClient,
            final ClientOverrideConfiguration overrideConfiguration) {

        if (configProps.containsKey(AWSConfigConstants.AWS_ENDPOINT)) {
            final URI endpointOverride =
                    URI.create(configProps.getProperty(AWSConfigConstants.AWS_ENDPOINT));
            clientBuilder.endpointOverride(endpointOverride);
        }

        return clientBuilder
                .httpClient(httpClient)
                .overrideConfiguration(overrideConfiguration)
                .credentialsProvider(getCredentialsProvider(configProps))
                .region(getRegion(configProps))
                .build();
    }

    public static boolean isRecoverableException(Exception e) {
        Throwable cause = e.getCause();
        return cause instanceof LimitExceededException
                || cause instanceof ProvisionedThroughputExceededException;
    }
}
