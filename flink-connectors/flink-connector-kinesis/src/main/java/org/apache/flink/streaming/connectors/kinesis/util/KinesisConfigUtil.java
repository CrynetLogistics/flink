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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utilities for Flink Kinesis connector configuration. */
@Internal
public class KinesisConfigUtil extends KinesisConfigGeneralUtil {

    /** Default value for ThreadingModel. */
    protected static final KinesisProducerConfiguration.ThreadingModel DEFAULT_THREADING_MODEL =
            KinesisProducerConfiguration.ThreadingModel.POOLED;

    /**
     * Validate configuration properties for {@code FlinkKinesisProducer}, and return a constructed
     * KinesisProducerConfiguration.
     */
    public static KinesisProducerConfiguration getValidatedProducerConfiguration(
            Properties config) {
        checkNotNull(config, "config can not be null");

        validateAwsConfiguration(config);

        if (!config.containsKey(AWSConfigConstants.AWS_REGION)) {
            // per requirement in Amazon Kinesis Producer Library
            throw new IllegalArgumentException(
                    String.format(
                            "For FlinkKinesisProducer AWS region ('%s') must be set in the config.",
                            AWSConfigConstants.AWS_REGION));
        }

        KinesisProducerConfiguration kpc = KinesisProducerConfiguration.fromProperties(config);
        kpc.setRegion(config.getProperty(AWSConfigConstants.AWS_REGION));

        kpc.setCredentialsProvider(AWSUtil.getCredentialsProvider(config));

        // we explicitly lower the credential refresh delay (default is 5 seconds)
        // to avoid an ignorable interruption warning that occurs when shutting down the
        // KPL client. See https://github.com/awslabs/amazon-kinesis-producer/issues/10.
        kpc.setCredentialsRefreshDelay(100);

        // Override default values if they aren't specified by users
        if (!config.containsKey(RATE_LIMIT)) {
            kpc.setRateLimit(DEFAULT_RATE_LIMIT);
        }
        if (!config.containsKey(THREADING_MODEL)) {
            kpc.setThreadingModel(DEFAULT_THREADING_MODEL);
        }
        if (!config.containsKey(THREAD_POOL_SIZE)) {
            kpc.setThreadPoolSize(DEFAULT_THREAD_POOL_SIZE);
        }

        return kpc;
    }
}
