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
import org.apache.flink.streaming.connectors.kinesis.config.ProducerConfigConstants;

import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

/** Tests for KinesisConfigUtil. */
@RunWith(PowerMockRunner.class)
public class KinesisConfigUtilTest {

    @Rule public ExpectedException exception = ExpectedException.none();

    // ----------------------------------------------------------------------
    // getValidatedProducerConfiguration() tests
    // ----------------------------------------------------------------------

    @Test
    public void testUnparsableLongForProducerConfiguration() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Error trying to set field RateLimit with the value 'unparsableLong'");

        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        testConfig.setProperty("RateLimit", "unparsableLong");

        KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);
    }

    @Test
    public void testRateLimitInProducerConfiguration() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        KinesisProducerConfiguration kpc =
                KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

        assertEquals(100, kpc.getRateLimit());

        testConfig.setProperty(KinesisConfigUtil.RATE_LIMIT, "150");
        kpc = KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

        assertEquals(150, kpc.getRateLimit());
    }

    @Test
    public void testThreadingModelInProducerConfiguration() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        KinesisProducerConfiguration kpc =
                KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

        assertEquals(KinesisProducerConfiguration.ThreadingModel.POOLED, kpc.getThreadingModel());

        testConfig.setProperty(KinesisConfigUtil.THREADING_MODEL, "PER_REQUEST");
        kpc = KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

        assertEquals(
                KinesisProducerConfiguration.ThreadingModel.PER_REQUEST, kpc.getThreadingModel());
    }

    @Test
    public void testThreadPoolSizeInProducerConfiguration() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        KinesisProducerConfiguration kpc =
                KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

        assertEquals(10, kpc.getThreadPoolSize());

        testConfig.setProperty(KinesisConfigUtil.THREAD_POOL_SIZE, "12");
        kpc = KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

        assertEquals(12, kpc.getThreadPoolSize());
    }

    @Test
    public void testReplaceDeprecatedKeys() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        // these deprecated keys should be replaced
        testConfig.setProperty(ProducerConfigConstants.AGGREGATION_MAX_COUNT, "1");
        testConfig.setProperty(ProducerConfigConstants.COLLECTION_MAX_COUNT, "2");
        Properties replacedConfig = KinesisConfigUtil.replaceDeprecatedProducerKeys(testConfig);

        assertEquals("1", replacedConfig.getProperty(KinesisConfigUtil.AGGREGATION_MAX_COUNT));
        assertEquals("2", replacedConfig.getProperty(KinesisConfigUtil.COLLECTION_MAX_COUNT));
    }

    @Test
    public void testCorrectlySetRegionInProducerConfiguration() {
        String region = "us-east-1";
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, region);
        KinesisProducerConfiguration kpc =
                KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

        assertEquals("incorrect region", region, kpc.getRegion());
    }

    @Test
    public void testMissingAwsRegionInProducerConfig() {
        String expectedMessage =
                String.format(
                        "For FlinkKinesisProducer AWS region ('%s') must be set in the config.",
                        AWSConfigConstants.AWS_REGION);
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(expectedMessage);

        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
        testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);
    }
}
