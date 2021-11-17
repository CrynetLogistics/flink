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
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.utils.AttributeMap;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;

/** Some general utilities specific to Amazon Web Service. */
@Internal
public class AWSGeneralUtil {

    /**
     * Determines and returns the credential provider type from the given properties.
     *
     * @return the credential provider type
     */
    public static CredentialProvider getCredentialProviderType(
            final Properties configProps, final String configPrefix) {
        if (!configProps.containsKey(configPrefix)) {
            if (configProps.containsKey(AWSConfigConstants.accessKeyId(configPrefix))
                    && configProps.containsKey(AWSConfigConstants.secretKey(configPrefix))) {
                // if the credential provider type is not specified, but the Access Key ID and
                // Secret Key are given, it will default to BASIC
                return CredentialProvider.BASIC;
            } else {
                // if the credential provider type is not specified, it will default to AUTO
                return CredentialProvider.AUTO;
            }
        } else {
            return CredentialProvider.valueOf(configProps.getProperty(configPrefix));
        }
    }

    /**
     * Return a {@link AwsCredentialsProvider} instance corresponding to the configuration
     * properties.
     *
     * @param configProps the configuration properties
     * @return The corresponding AWS Credentials Provider instance
     */
    public static AwsCredentialsProvider getCredentialsProvider(final Properties configProps) {
        return getCredentialsProvider(configProps, AWSConfigConstants.AWS_CREDENTIALS_PROVIDER);
    }

    public static AwsCredentialsProvider getCredentialsProvider(
            final Properties configProps, final String configPrefix) {
        CredentialProvider credentialProviderType =
                getCredentialProviderType(configProps, configPrefix);

        switch (credentialProviderType) {
            case ENV_VAR:
                return EnvironmentVariableCredentialsProvider.create();

            case SYS_PROP:
                return SystemPropertyCredentialsProvider.create();

            case PROFILE:
                return getProfileCredentialProvider(configProps, configPrefix);

            case BASIC:
                return () ->
                        AwsBasicCredentials.create(
                                configProps.getProperty(
                                        AWSConfigConstants.accessKeyId(configPrefix)),
                                configProps.getProperty(
                                        AWSConfigConstants.secretKey(configPrefix)));

            case ASSUME_ROLE:
                return getAssumeRoleCredentialProvider(configProps, configPrefix);

            case WEB_IDENTITY_TOKEN:
                return getWebIdentityTokenFileCredentialsProvider(
                        WebIdentityTokenFileCredentialsProvider.builder(),
                        configProps,
                        configPrefix);

            case AUTO:
                return DefaultCredentialsProvider.create();

            default:
                throw new IllegalArgumentException(
                        "Credential provider not supported: " + credentialProviderType);
        }
    }

    public static AwsCredentialsProvider getProfileCredentialProvider(
            final Properties configProps, final String configPrefix) {
        String profileName =
                configProps.getProperty(AWSConfigConstants.profileName(configPrefix), null);

        ProfileCredentialsProvider.Builder profileBuilder =
                ProfileCredentialsProvider.builder().profileName(profileName);

        Optional.ofNullable(configProps.getProperty(AWSConfigConstants.profilePath(configPrefix)))
                .map(Paths::get)
                .ifPresent(
                        path ->
                                profileBuilder.profileFile(
                                        ProfileFile.builder()
                                                .type(ProfileFile.Type.CREDENTIALS)
                                                .content(path)
                                                .build()));

        return profileBuilder.build();
    }

    private static AwsCredentialsProvider getAssumeRoleCredentialProvider(
            final Properties configProps, final String configPrefix) {
        return StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(
                        AssumeRoleRequest.builder()
                                .roleArn(
                                        configProps.getProperty(
                                                AWSConfigConstants.roleArn(configPrefix)))
                                .roleSessionName(
                                        configProps.getProperty(
                                                AWSConfigConstants.roleSessionName(configPrefix)))
                                .externalId(
                                        configProps.getProperty(
                                                AWSConfigConstants.externalId(configPrefix)))
                                .build())
                .stsClient(
                        StsClient.builder()
                                .credentialsProvider(
                                        getCredentialsProvider(
                                                configProps,
                                                AWSConfigConstants.roleCredentialsProvider(
                                                        configPrefix)))
                                .region(getRegion(configProps))
                                .build())
                .build();
    }

    @VisibleForTesting
    static AwsCredentialsProvider getWebIdentityTokenFileCredentialsProvider(
            final WebIdentityTokenFileCredentialsProvider.Builder webIdentityBuilder,
            final Properties configProps,
            final String configPrefix) {

        Optional.ofNullable(configProps.getProperty(AWSConfigConstants.roleArn(configPrefix)))
                .ifPresent(webIdentityBuilder::roleArn);

        Optional.ofNullable(
                        configProps.getProperty(AWSConfigConstants.roleSessionName(configPrefix)))
                .ifPresent(webIdentityBuilder::roleSessionName);

        Optional.ofNullable(
                        configProps.getProperty(
                                AWSConfigConstants.webIdentityTokenFile(configPrefix)))
                .map(Paths::get)
                .ifPresent(webIdentityBuilder::webIdentityTokenFile);

        return webIdentityBuilder.build();
    }

    public static SdkAsyncHttpClient createHttpClient(
            final AttributeMap config, final NettyNioAsyncHttpClient.Builder httpClientBuilder) {
        return httpClientBuilder.buildWithDefaults(config);
    }

    /**
     * Creates a {@link Region} object from the given Properties.
     *
     * @param configProps the properties containing the region
     * @return the region specified by the properties
     */
    public static Region getRegion(final Properties configProps) {
        return Region.of(configProps.getProperty(AWSConfigConstants.AWS_REGION));
    }
}
