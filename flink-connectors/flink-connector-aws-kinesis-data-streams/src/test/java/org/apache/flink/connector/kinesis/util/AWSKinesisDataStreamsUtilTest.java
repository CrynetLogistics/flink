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

import org.junit.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.internal.NettyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;

import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.connector.kinesis.config.AsyncProducerConfigConstants.DEFAULT_HTTP_CLIENT_MAX_CONCURRENCY;
import static org.apache.flink.connector.kinesis.config.AsyncProducerConfigConstants.DEFAULT_HTTP_CLIENT_READ_TIMEOUT;
import static org.apache.flink.connector.kinesis.config.AsyncProducerConfigConstants.DEFAULT_TRUST_ALL_CERTIFICATES;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_REGION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.http.Protocol.HTTP2;

/** Tests for {@link AWSKinesisDataStreamsUtil}. */
public class AWSKinesisDataStreamsUtilTest {
    private static final boolean LEGACY_CONNECTOR_PLACEHOLDER = false;

    @Test
    public void testCreateKinesisAsyncClient() {
        Properties properties = TestUtil.properties(AWS_REGION, "eu-west-2");
        KinesisAsyncClientBuilder builder = mockKinesisAsyncClientBuilder();
        ClientOverrideConfiguration clientOverrideConfiguration =
                ClientOverrideConfiguration.builder().build();
        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder().build();

        AWSKinesisDataStreamsUtil.createKinesisAsyncClient(
                properties, builder, httpClient, clientOverrideConfiguration);

        verify(builder).overrideConfiguration(clientOverrideConfiguration);
        verify(builder).httpClient(httpClient);
        verify(builder).region(Region.of("eu-west-2"));
        verify(builder)
                .credentialsProvider(argThat(cp -> cp instanceof DefaultCredentialsProvider));
        verify(builder, never()).endpointOverride(any());
    }

    @Test
    public void testCreateKinesisAsyncClientWithEndpointOverride() {
        Properties properties = TestUtil.properties(AWS_REGION, "eu-west-2");
        properties.setProperty(AWS_ENDPOINT, "https://localhost");

        KinesisAsyncClientBuilder builder = mockKinesisAsyncClientBuilder();
        ClientOverrideConfiguration clientOverrideConfiguration =
                ClientOverrideConfiguration.builder().build();
        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder().build();

        AWSKinesisDataStreamsUtil.createKinesisAsyncClient(
                properties, builder, httpClient, clientOverrideConfiguration);

        verify(builder).endpointOverride(URI.create("https://localhost"));
    }

    @Test
    public void testCreateNettyHttpClientWithDefaults() throws Exception {
        NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder();
        SdkAsyncHttpClient httpClient = AWSKinesisDataStreamsUtil.createHttpClient(builder);
        NettyConfiguration nettyConfiguration = TestUtil.getNettyConfiguration(httpClient);

        assertEquals(DEFAULT_HTTP_CLIENT_MAX_CONCURRENCY, nettyConfiguration.maxConnections());
        assertEquals(
                DEFAULT_HTTP_CLIENT_READ_TIMEOUT.toMillis(),
                nettyConfiguration.readTimeoutMillis());
        assertEquals(
                AWSKinesisDataStreamsUtil.CONNECTION_ACQUISITION_TIMEOUT.toMillis(),
                nettyConfiguration.connectionAcquireTimeoutMillis());
        assertEquals(HTTP2, nettyConfiguration.attribute(SdkHttpConfigurationOption.PROTOCOL));
        assertEquals(DEFAULT_TRUST_ALL_CERTIFICATES, nettyConfiguration.trustAllCertificates());
        assertEquals(0, nettyConfiguration.connectionTtlMillis());
    }

    @Test
    public void testClientOverrideConfigurationWithDefaults() {
        SdkClientConfiguration clientConfiguration = SdkClientConfiguration.builder().build();

        ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

        AWSKinesisDataStreamsUtil.createClientOverrideConfiguration(
                clientConfiguration, builder, LEGACY_CONNECTOR_PLACEHOLDER);

        verify(builder).build();
        verify(builder)
                .putAdvancedOption(
                        SdkAdvancedClientOption.USER_AGENT_PREFIX,
                        AWSKinesisDataStreamsUtil.formatFlinkUserAgentPrefix(false));
        verify(builder).putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX, null);
        verify(builder, never()).apiCallAttemptTimeout(any());
        verify(builder, never()).apiCallTimeout(any());
    }

    @Test
    public void testClientOverrideConfigurationUserAgentSuffix() {
        SdkClientConfiguration clientConfiguration =
                SdkClientConfiguration.builder()
                        .option(SdkAdvancedClientOption.USER_AGENT_SUFFIX, "suffix")
                        .build();

        ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

        AWSKinesisDataStreamsUtil.createClientOverrideConfiguration(
                clientConfiguration, builder, LEGACY_CONNECTOR_PLACEHOLDER);

        verify(builder).putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX, "suffix");
    }

    @Test
    public void testClientOverrideConfigurationApiCallAttemptTimeout() {
        SdkClientConfiguration clientConfiguration =
                SdkClientConfiguration.builder()
                        .option(SdkClientOption.API_CALL_ATTEMPT_TIMEOUT, Duration.ofMillis(500))
                        .build();

        ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

        AWSKinesisDataStreamsUtil.createClientOverrideConfiguration(
                clientConfiguration, builder, LEGACY_CONNECTOR_PLACEHOLDER);

        verify(builder).apiCallAttemptTimeout(Duration.ofMillis(500));
    }

    @Test
    public void testClientOverrideConfigurationApiCallTimeout() {
        SdkClientConfiguration clientConfiguration =
                SdkClientConfiguration.builder()
                        .option(SdkClientOption.API_CALL_TIMEOUT, Duration.ofMillis(600))
                        .build();

        ClientOverrideConfiguration.Builder builder = mockClientOverrideConfigurationBuilder();

        AWSKinesisDataStreamsUtil.createClientOverrideConfiguration(
                clientConfiguration, builder, LEGACY_CONNECTOR_PLACEHOLDER);

        verify(builder).apiCallTimeout(Duration.ofMillis(600));
    }

    @Test
    public void testIsRecoverableExceptionForRecoverable() {
        Exception recoverable = LimitExceededException.builder().build();
        assertTrue(
                AWSKinesisDataStreamsUtil.isRecoverableException(
                        new ExecutionException(recoverable)));
    }

    @Test
    public void testIsRecoverableExceptionForNonRecoverable() {
        Exception nonRecoverable = new IllegalArgumentException("abc");
        assertFalse(
                AWSKinesisDataStreamsUtil.isRecoverableException(
                        new ExecutionException(nonRecoverable)));
    }

    @Test
    public void testIsRecoverableExceptionForRuntimeExceptionWrappingRecoverable() {
        Exception recoverable = LimitExceededException.builder().build();
        Exception runtime = new RuntimeException("abc", recoverable);
        assertTrue(AWSKinesisDataStreamsUtil.isRecoverableException(runtime));
    }

    @Test
    public void testIsRecoverableExceptionForRuntimeExceptionWrappingNonRecoverable() {
        Exception nonRecoverable = new IllegalArgumentException("abc");
        Exception runtime = new RuntimeException("abc", nonRecoverable);
        assertFalse(AWSKinesisDataStreamsUtil.isRecoverableException(runtime));
    }

    @Test
    public void testIsRecoverableExceptionForNullCause() {
        Exception nonRecoverable = new IllegalArgumentException("abc");
        assertFalse(AWSKinesisDataStreamsUtil.isRecoverableException(nonRecoverable));
    }

    private KinesisAsyncClientBuilder mockKinesisAsyncClientBuilder() {
        KinesisAsyncClientBuilder builder = mock(KinesisAsyncClientBuilder.class);
        when(builder.overrideConfiguration(any(ClientOverrideConfiguration.class)))
                .thenReturn(builder);
        when(builder.httpClient(any())).thenReturn(builder);
        when(builder.credentialsProvider(any())).thenReturn(builder);
        when(builder.region(any())).thenReturn(builder);

        return builder;
    }

    private ClientOverrideConfiguration.Builder mockClientOverrideConfigurationBuilder() {
        ClientOverrideConfiguration.Builder builder =
                mock(ClientOverrideConfiguration.Builder.class);
        when(builder.putAdvancedOption(any(), any())).thenReturn(builder);
        when(builder.apiCallAttemptTimeout(any())).thenReturn(builder);
        when(builder.apiCallTimeout(any())).thenReturn(builder);

        return builder;
    }
}
