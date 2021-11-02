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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;

import java.util.Map;

class OperatorIOMetricGroupMock implements OperatorIOMetricGroup {

    private final Counter numRecordsOutCounter = new SimpleCounter();
    private final Counter numBytesOutCounter = new SimpleCounter();

    @Override
    public Counter getNumRecordsInCounter() {
        return null;
    }

    @Override
    public Counter getNumRecordsOutCounter() {
        return numRecordsOutCounter;
    }

    @Override
    public Counter getNumBytesInCounter() {
        return null;
    }

    @Override
    public Counter getNumBytesOutCounter() {
        return numBytesOutCounter;
    }

    @Override
    public Counter counter(String s) {
        return null;
    }

    @Override
    public <C extends Counter> C counter(String s, C c) {
        return null;
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String s, G g) {
        return null;
    }

    @Override
    public <H extends Histogram> H histogram(String s, H h) {
        return null;
    }

    @Override
    public <M extends Meter> M meter(String s, M m) {
        return null;
    }

    @Override
    public MetricGroup addGroup(String s) {
        return null;
    }

    @Override
    public MetricGroup addGroup(String s, String s1) {
        return null;
    }

    @Override
    public String[] getScopeComponents() {
        return new String[0];
    }

    @Override
    public Map<String, String> getAllVariables() {
        return null;
    }

    @Override
    public String getMetricIdentifier(String s) {
        return null;
    }

    @Override
    public String getMetricIdentifier(String s, CharacterFilter characterFilter) {
        return null;
    }
}
