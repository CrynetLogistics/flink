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

package org.apache.flink.connector.kinesis.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.kinesis.util.AWSKinesisDataStreamsUtil;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import software.amazon.awssdk.http.Protocol;

import java.time.Duration;

/** Defaults for {@link AWSKinesisDataStreamsUtil}. */
@PublicEvolving
public class AWSKinesisDataStreamsConfigConstants extends AWSConfigConstants {
    public static final int DEFAULT_HTTP_CLIENT_MAX_CONCURRENCY = 10_000;

    public static final Duration DEFAULT_HTTP_CLIENT_READ_TIMEOUT = Duration.ofMinutes(6);

    public static final boolean DEFAULT_TRUST_ALL_CERTIFICATES = false;

    public static final Protocol DEFAULT_HTTP_PROTOCOL = Protocol.HTTP2;

    public static final boolean DEFAULT_LEGACY_CONNECTOR = false;
}
