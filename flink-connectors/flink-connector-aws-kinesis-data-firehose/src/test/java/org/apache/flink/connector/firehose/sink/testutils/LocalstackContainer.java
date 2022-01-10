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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;

public class LocalstackContainer extends GenericContainer<LocalstackContainer> {

    private static final int PORT = 4566;

    public LocalstackContainer(DockerImageName imageName) {
        super(imageName);
        withExposedPorts(PORT);
        setPortBindings(Collections.singletonList(String.format("127.0.0.1:%s:%s", PORT, PORT)));
    }

    public String getEndpoint() {
        return String.format("https://localhost:%s", PORT);
    }
}
