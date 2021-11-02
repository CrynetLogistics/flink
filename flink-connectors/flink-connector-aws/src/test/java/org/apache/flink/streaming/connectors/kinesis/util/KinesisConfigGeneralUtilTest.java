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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.testutils.GeneralTestUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_HTTP_CLIENT_MAX_CONCURRENCY;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests for KinesisConfigGeneralUtil. */
@RunWith(PowerMockRunner.class)
public class KinesisConfigGeneralUtilTest {

    @Rule public ExpectedException exception = ExpectedException.none();

    // ----------------------------------------------------------------------
    // validateAwsConfiguration() tests
    // ----------------------------------------------------------------------

    @Test
    public void testUnrecognizableAwsRegionInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Invalid AWS region");

        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "wrongRegionId");
        testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
        testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        KinesisConfigGeneralUtil.validateAwsConfiguration(testConfig);
    }

    @Test
    public void testCredentialProviderTypeSetToBasicButNoCredentialSetInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Please set values for AWS Access Key ID ('"
                        + AWSConfigConstants.AWS_ACCESS_KEY_ID
                        + "') "
                        + "and Secret Key ('"
                        + AWSConfigConstants.AWS_SECRET_ACCESS_KEY
                        + "') when using the BASIC AWS credential provider type.");

        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");

        KinesisConfigGeneralUtil.validateAwsConfiguration(testConfig);
    }

    @Test
    public void testUnrecognizableCredentialProviderTypeInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Invalid AWS Credential Provider Type");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "wrongProviderType");

        KinesisConfigGeneralUtil.validateAwsConfiguration(testConfig);
    }

    // ----------------------------------------------------------------------
    // validateRecordPublisherType() tests
    // ----------------------------------------------------------------------
    @Test
    public void testNoRecordPublisherTypeInConfig() {
        Properties testConfig = GeneralTestUtils.getStandardProperties();
        ConsumerConfigConstants.RecordPublisherType recordPublisherType =
                KinesisConfigGeneralUtil.validateRecordPublisherType(testConfig);
        assertEquals(recordPublisherType, ConsumerConfigConstants.RecordPublisherType.POLLING);
    }

    @Test
    public void testUnrecognizableRecordPublisherTypeInConfig() {
        String errorMessage =
                Arrays.stream(ConsumerConfigConstants.RecordPublisherType.values())
                        .map(Enum::name)
                        .collect(Collectors.joining(", "));
        String msg =
                "Invalid record publisher type in stream set in config. Valid values are: "
                        + errorMessage;
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(msg);
        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, "unrecognizableRecordPublisher");

        KinesisConfigGeneralUtil.validateRecordPublisherType(testConfig);
    }

    // ----------------------------------------------------------------------
    // validateEfoConfiguration() tests
    // ----------------------------------------------------------------------

    @Test
    public void testNoEfoRegistrationTypeInConfig() {
        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.RECORD_PUBLISHER_TYPE,
                ConsumerConfigConstants.RecordPublisherType.EFO.toString());
        testConfig.setProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME, "fakedconsumername");

        KinesisConfigGeneralUtil.validateEfoConfiguration(testConfig, new ArrayList<>());
    }

    @Test
    public void testUnrecognizableEfoRegistrationTypeInConfig() {
        String errorMessage =
                Arrays.stream(ConsumerConfigConstants.EFORegistrationType.values())
                        .map(Enum::name)
                        .collect(Collectors.joining(", "));
        String msg =
                "Invalid efo registration type in stream set in config. Valid values are: "
                        + errorMessage;
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(msg);
        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.RECORD_PUBLISHER_TYPE,
                ConsumerConfigConstants.RecordPublisherType.EFO.toString());
        testConfig.setProperty(
                ConsumerConfigConstants.EFO_REGISTRATION_TYPE, "unrecogonizeEforegistrationtype");

        KinesisConfigGeneralUtil.validateEfoConfiguration(testConfig, new ArrayList<>());
    }

    @Test
    public void testNoneTypeEfoRegistrationTypeWithNoMatchedStreamInConfig() {
        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.RECORD_PUBLISHER_TYPE,
                ConsumerConfigConstants.RecordPublisherType.EFO.toString());
        testConfig.setProperty(
                ConsumerConfigConstants.EFO_REGISTRATION_TYPE,
                ConsumerConfigConstants.EFORegistrationType.NONE.toString());

        KinesisConfigGeneralUtil.validateEfoConfiguration(testConfig, new ArrayList<>());
    }

    @Test
    public void testNoneTypeEfoRegistrationTypeWithEnoughMatchedStreamInConfig() {

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.RECORD_PUBLISHER_TYPE,
                ConsumerConfigConstants.RecordPublisherType.EFO.toString());
        testConfig.setProperty(
                ConsumerConfigConstants.EFO_REGISTRATION_TYPE,
                ConsumerConfigConstants.EFORegistrationType.NONE.toString());
        testConfig.setProperty(
                ConsumerConfigConstants.EFO_CONSUMER_ARN_PREFIX + ".stream1", "fakedArn1");
        List<String> streams = new ArrayList<>();
        streams.add("stream1");
        KinesisConfigGeneralUtil.validateEfoConfiguration(testConfig, streams);
    }

    @Test
    public void testNoneTypeEfoRegistrationTypeWithOverEnoughMatchedStreamInConfig() {
        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.RECORD_PUBLISHER_TYPE,
                ConsumerConfigConstants.RecordPublisherType.EFO.toString());
        testConfig.setProperty(
                ConsumerConfigConstants.EFO_REGISTRATION_TYPE,
                ConsumerConfigConstants.EFORegistrationType.NONE.toString());
        testConfig.setProperty(
                ConsumerConfigConstants.EFO_CONSUMER_ARN_PREFIX + ".stream1", "fakedArn1");
        testConfig.setProperty(
                ConsumerConfigConstants.EFO_CONSUMER_ARN_PREFIX + ".stream2", "fakedArn2");
        List<String> streams = new ArrayList<>();
        streams.add("stream1");
        KinesisConfigGeneralUtil.validateEfoConfiguration(testConfig, streams);
    }

    @Test
    public void testNoneTypeEfoRegistrationTypeWithNotEnoughMatchedStreamInConfig() {
        String msg =
                "Invalid efo consumer arn settings for not providing consumer arns: "
                        + ConsumerConfigConstants.EFO_CONSUMER_ARN_PREFIX
                        + ".stream2";
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(msg);
        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.RECORD_PUBLISHER_TYPE,
                ConsumerConfigConstants.RecordPublisherType.EFO.toString());
        testConfig.setProperty(
                ConsumerConfigConstants.EFO_REGISTRATION_TYPE,
                ConsumerConfigConstants.EFORegistrationType.NONE.toString());
        testConfig.setProperty(
                ConsumerConfigConstants.EFO_CONSUMER_ARN_PREFIX + ".stream1", "fakedArn1");
        List<String> streams = Arrays.asList("stream1", "stream2");
        KinesisConfigGeneralUtil.validateEfoConfiguration(testConfig, streams);
    }

    @Test
    public void testValidateEfoMaxConcurrency() {
        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(EFO_HTTP_CLIENT_MAX_CONCURRENCY, "55");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testValidateEfoMaxConcurrencyNonNumeric() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for EFO HTTP client max concurrency. Must be positive.");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(EFO_HTTP_CLIENT_MAX_CONCURRENCY, "abc");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testValidateEfoMaxConcurrencyNegative() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for EFO HTTP client max concurrency. Must be positive.");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(EFO_HTTP_CLIENT_MAX_CONCURRENCY, "-1");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    // ----------------------------------------------------------------------
    // validateConsumerConfiguration() tests
    // ----------------------------------------------------------------------

    @Test
    public void testNoAwsRegionOrEndpointInConsumerConfig() {
        String expectedMessage =
                String.format(
                        "For FlinkKinesisConsumer AWS region ('%s') and/or AWS endpoint ('%s') must be set in the config.",
                        AWSConfigConstants.AWS_REGION, AWSConfigConstants.AWS_ENDPOINT);
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(expectedMessage);

        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
        testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testAwsRegionAndEndpointInConsumerConfig() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        testConfig.setProperty(AWSConfigConstants.AWS_ENDPOINT, "fake");
        testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
        testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testAwsRegionInConsumerConfig() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
        testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testEndpointInConsumerConfig() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_ENDPOINT, "fake");
        testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
        testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnrecognizableStreamInitPositionTypeInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Invalid initial position in stream");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(
                ConsumerConfigConstants.STREAM_INITIAL_POSITION, "wrongInitPosition");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testStreamInitPositionTypeSetToAtTimestampButNoInitTimestampSetInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Please set value for initial timestamp ('"
                        + ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP
                        + "') when using AT_TIMESTAMP initial position.");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDateforInitialTimestampInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "unparsableDate");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testIllegalValuEforInitialTimestampInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "-1.0");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testDateStringForValidateOptionDateProperty() {
        String timestamp = "2016-04-04T19:58:46.480-00:00";

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, timestamp);

        try {
            KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testUnixTimestampForValidateOptionDateProperty() {
        String unixTimestamp = "1459799926.480";

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, unixTimestamp);

        try {
            KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testInvalidPatternForInitialTimestampInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "2016-03-14");
        testConfig.setProperty(
                ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT, "InvalidPattern");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDatEforUserDefinedDatEformatForInitialTimestampInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "stillUnparsable");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT, "yyyy-MM-dd");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testDateStringForUserDefinedDatEformatForValidateOptionDateProperty() {
        String unixTimestamp = "2016-04-04";
        String pattern = "yyyy-MM-dd";

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, unixTimestamp);
        testConfig.setProperty(ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT, pattern);

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForListShardsBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for list shards operation base backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.LIST_SHARDS_BACKOFF_BASE, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForListShardsBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for list shards operation max backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.LIST_SHARDS_BACKOFF_MAX, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforListShardsBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for list shards operation backoff exponential constant");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForGetRecordsRetriesInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum retry attempts for getRecords shard operation");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_RETRIES, "unparsableInt");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForGetRecordsMaxCountInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum records per getRecords shard operation");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX, "unparsableInt");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForGetRecordsBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for get records operation base backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForGetRecordsBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for get records operation max backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforGetRecordsBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for get records operation backoff exponential constant");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForGetRecordsIntervalMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for getRecords sleep interval in milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForGetShardIteratorRetriesInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum retry attempts for getShardIterator shard operation");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.SHARD_GETITERATOR_RETRIES, "unparsableInt");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForGetShardIteratorBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for get shard iterator operation base backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForGetShardIteratorBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for get shard iterator operation max backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_MAX, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforGetShardIteratorBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for get shard iterator operation backoff exponential constant");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForShardDiscoveryIntervalMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for shard discovery sleep interval in milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testParseStreamTimestampStartingPositionUsingDefaultFormat() throws Exception {
        String timestamp = "2020-08-13T09:18:00.0+01:00";
        Date expectedTimestamp =
                new SimpleDateFormat(DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT).parse(timestamp);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);

        Date actualimestamp =
                KinesisConfigGeneralUtil.parseStreamTimestampStartingPosition(consumerProperties);

        assertEquals(expectedTimestamp, actualimestamp);
    }

    @Test
    public void testParseStreamTimestampStartingPositionUsingCustomFormat() throws Exception {
        String format = "yyyy-MM-dd'T'HH:mm";
        String timestamp = "2020-08-13T09:23";
        Date expectedTimestamp = new SimpleDateFormat(format).parse(timestamp);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);
        consumerProperties.setProperty(STREAM_TIMESTAMP_DATE_FORMAT, format);

        Date actualimestamp =
                KinesisConfigGeneralUtil.parseStreamTimestampStartingPosition(consumerProperties);

        assertEquals(expectedTimestamp, actualimestamp);
    }

    @Test
    public void testParseStreamTimestampStartingPositionUsingParseError() {
        exception.expect(NumberFormatException.class);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, "bad");

        KinesisConfigGeneralUtil.parseStreamTimestampStartingPosition(consumerProperties);
    }

    @Test
    public void testParseStreamTimestampStartingPositionIllegalArgumentException() {
        exception.expect(IllegalArgumentException.class);

        Properties consumerProperties = new Properties();

        KinesisConfigGeneralUtil.parseStreamTimestampStartingPosition(consumerProperties);
    }

    public void testUnparsableIntForRegisterStreamRetriesInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum retry attempts for register stream operation");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.REGISTER_STREAM_RETRIES, "unparsableInt");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForRegisterStreamTimeoutInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum timeout for register stream consumer. Must be a valid non-negative integer value.");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.REGISTER_STREAM_TIMEOUT_SECONDS, "unparsableInt");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForRegisterStreamBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for register stream operation base backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_BASE, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForRegisterStreamBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for register stream operation max backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_MAX, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforRegisterStreamBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for register stream operation backoff exponential constant");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForDeRegisterStreamRetriesInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum retry attempts for deregister stream operation");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.DEREGISTER_STREAM_RETRIES, "unparsableInt");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForDeRegisterStreamTimeoutInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum timeout for deregister stream consumer. Must be a valid non-negative integer value.");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.DEREGISTER_STREAM_TIMEOUT_SECONDS, "unparsableInt");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForDeRegisterStreamBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for deregister stream operation base backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_BASE, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForDeRegisterStreamBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for deregister stream operation max backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_MAX, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforDeRegisterStreamBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for deregister stream operation backoff exponential constant");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForDescribeStreamConsumerRetriesInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum retry attempts for describe stream consumer operation");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_RETRIES, "unparsableInt");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForDescribeStreamConsumerBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for describe stream consumer operation base backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_BASE, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForDescribeStreamConsumerBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for describe stream consumer operation max backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_MAX, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforDescribeStreamConsumerBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for describe stream consumer operation backoff exponential constant");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForSubscribeToShardRetriesInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum retry attempts for subscribe to shard operation");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_RETRIES, "unparsableInt");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForSubscribeToShardBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for subscribe to shard operation base backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_BASE, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForSubscribeToShardBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for subscribe to shard operation max backoff milliseconds");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_MAX, "unparsableLong");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforSubscribeToShardBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for subscribe to shard operation backoff exponential constant");

        Properties testConfig = GeneralTestUtils.getStandardProperties();
        testConfig.setProperty(
                ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigGeneralUtil.validateConsumerConfiguration(testConfig);
    }
}
