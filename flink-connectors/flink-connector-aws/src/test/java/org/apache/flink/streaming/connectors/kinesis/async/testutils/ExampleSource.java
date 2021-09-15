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

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.base.Preconditions;

/**
 * An example source function that allows the user to customize the number of emitted elements, the
 * time between emissions, the total amount of time to keep alive after the last element and the
 * size of each element in bytes.
 *
 * <p>This source is used in the testing of KDS Sinks.
 */
public class ExampleSource extends RichSourceFunction<String> {
    private static final long serialVersionUID = 1L;
    private volatile boolean running = true;
    private int emittedCount = 1;
    private final int numToCountTo;
    private final int timeBetweenHitsMS;
    private final int keepAliveTimeAfterMS;
    private final String payload;

    public ExampleSource(
            int numToCountTo,
            int timeBetweenHitsMS,
            int keepAliveTimeAfterMS,
            int sizeOfEachMessageBytes) {
        this.numToCountTo = numToCountTo;
        this.timeBetweenHitsMS = timeBetweenHitsMS;
        this.keepAliveTimeAfterMS = keepAliveTimeAfterMS;
        Preconditions.checkArgument(
                sizeOfEachMessageBytes >= 25,
                "The minimum size of the message should be 25 bytes, i.e. that is "
                        + "the size of the message with an empty payload. Additional data in the payload is 1 byte per character.");
        payload = new String(new char[sizeOfEachMessageBytes - 25]).replace('\0', '*');
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        for (; this.running; Thread.sleep(timeBetweenHitsMS)) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(emittedMessage());
            }

            if (this.emittedCount < numToCountTo) {
                ++this.emittedCount;
            } else {
                Thread.sleep(keepAliveTimeAfterMS);
                return;
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    private String emittedMessage() {
        return String.format(
                "{\"%s\":%d, \"%s\":\"%s\"}", "count", this.emittedCount, "payload", payload);
    }
}
