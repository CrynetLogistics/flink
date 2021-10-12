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

package org.apache.flink.connector.base.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import java.io.Serializable;

/** Configuration for {@link AsyncSinkBase}. */
@Internal
public class AsyncSinkBaseConfig<InputT, RequestEntryT extends Serializable>
        implements Serializable {

    private final ElementConverter<InputT, RequestEntryT> elementConverter;
    private final int maxBatchSize;
    private final int maxInFlightRequests;
    private final int maxBufferedRequests;
    private final long flushOnBufferSizeInBytes;
    private final long maxTimeInBufferMS;

    public AsyncSinkBaseConfig(
            ElementConverter<InputT, RequestEntryT> elementConverter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long flushOnBufferSizeInBytes,
            long maxTimeInBufferMS) {
        this.elementConverter = elementConverter;
        this.maxBatchSize = maxBatchSize;
        this.maxInFlightRequests = maxInFlightRequests;
        this.maxBufferedRequests = maxBufferedRequests;
        this.flushOnBufferSizeInBytes = flushOnBufferSizeInBytes;
        this.maxTimeInBufferMS = maxTimeInBufferMS;
    }

    public ElementConverter<InputT, RequestEntryT> getElementConverter() {
        return elementConverter;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public int getMaxInFlightRequests() {
        return maxInFlightRequests;
    }

    public int getMaxBufferedRequests() {
        return maxBufferedRequests;
    }

    public long getFlushOnBufferSizeInBytes() {
        return flushOnBufferSizeInBytes;
    }

    public long getMaxTimeInBufferMS() {
        return maxTimeInBufferMS;
    }
}
