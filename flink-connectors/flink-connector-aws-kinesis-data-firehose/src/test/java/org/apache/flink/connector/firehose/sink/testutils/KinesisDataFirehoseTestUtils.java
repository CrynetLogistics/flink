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

package org.apache.flink.connector.firehose.sink.testutils;

import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.config.AWSUnifiedSinksConfigConstants;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.aws.util.AWSUnifiedSinksUtil;

import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamType;
import software.amazon.awssdk.services.firehose.model.ExtendedS3DestinationConfiguration;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.waiters.S3AsyncWaiter;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;

public class KinesisDataFirehoseTestUtils {

    public static S3AsyncClient makeS3Client(String endpoint) throws URISyntaxException {
        return S3AsyncClient.builder()
                .httpClient(getHttpClient(endpoint))
                .region(Region.AP_SOUTHEAST_1)
                .endpointOverride(new URI(endpoint))
                .build();
    }

    public static FirehoseAsyncClient makeFirehoseClient(String endpoint) throws URISyntaxException {
        return AWSUnifiedSinksUtil.createAwsAsyncClient(
                getConfig(endpoint),
                getHttpClient(endpoint),
                FirehoseAsyncClient.builder()
                        .endpointOverride(new URI(endpoint)),
                AWSUnifiedSinksConfigConstants.BASE_KINESIS_USER_AGENT_PREFIX_FORMAT,
                AWSUnifiedSinksConfigConstants.KINESIS_CLIENT_USER_AGENT_PREFIX);
    }

    public static IamAsyncClient createIamClient(String endpoint) throws URISyntaxException {
        return IamAsyncClient.builder()
                .httpClient(getHttpClient(endpoint))
                .region(Region.AWS_GLOBAL)
                .endpointOverride(new URI(endpoint))
                .build();
    }

    public static Properties getConfig(String endpoint) {
        Properties config = new Properties();
        config.setProperty(AWSConfigConstants.AWS_REGION, Region.AP_SOUTHEAST_1.toString());
        config.setProperty(AWSConfigConstants.AWS_ENDPOINT, endpoint);
        config.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
        config.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretAccessKey");
        config.setProperty(TRUST_ALL_CERTIFICATES, "true");
        config.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");
        return config;
    }

    public static SdkAsyncHttpClient getHttpClient(String endpoint) {
        return AWSGeneralUtil.createAsyncHttpClient(getConfig(endpoint));
    }

    public static void makeBucket(S3AsyncClient s3Client, String bucketName)
            throws ExecutionException, InterruptedException {
        CreateBucketRequest bucketRequest =
                CreateBucketRequest.builder().bucket(bucketName).build();
        s3Client.createBucket(bucketRequest);

        HeadBucketRequest bucketRequestWait =
                HeadBucketRequest.builder().bucket(bucketName).build();

        S3AsyncWaiter s3Waiter = s3Client.waiter();
        CompletableFuture<WaiterResponse<HeadBucketResponse>> waiterResponseFuture =
                s3Waiter.waitUntilBucketExists(bucketRequestWait);

        waiterResponseFuture.get();
    }

    public static void createIAMRole(IamAsyncClient iam, String roleName)
            throws ExecutionException, InterruptedException {
        CreateRoleRequest request = CreateRoleRequest.builder().roleName(roleName).build();

        CompletableFuture<CreateRoleResponse> responseFuture = iam.createRole(request);
        responseFuture.get();
    }

    public static void createDeliveryStream(
            String deliveryStreamName,
            String bucketName,
            String roleARN,
            FirehoseAsyncClient firehoseAsyncClient)
            throws ExecutionException, InterruptedException {
        ExtendedS3DestinationConfiguration s3Config =
                ExtendedS3DestinationConfiguration.builder()
                        .bucketARN(bucketName)
                        .roleARN(roleARN)
                        .build();
        CreateDeliveryStreamRequest request =
                CreateDeliveryStreamRequest.builder()
                        .deliveryStreamName(deliveryStreamName)
                        .extendedS3DestinationConfiguration(s3Config)
                        .deliveryStreamType(DeliveryStreamType.DIRECT_PUT)
                        .build();

        CompletableFuture<CreateDeliveryStreamResponse> deliveryStream =
                firehoseAsyncClient.createDeliveryStream(request);
        deliveryStream.get();
    }

    public static List<S3Object> listBucketObjects(S3AsyncClient s3, String bucketName)
            throws ExecutionException, InterruptedException {
        ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(bucketName).build();
        CompletableFuture<ListObjectsResponse> res = s3.listObjects(listObjects);
        return res.get().contents();
    }

}
