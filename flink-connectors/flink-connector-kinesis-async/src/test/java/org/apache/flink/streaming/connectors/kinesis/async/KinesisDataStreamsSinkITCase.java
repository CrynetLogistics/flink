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

package org.apache.flink.streaming.connectors.kinesis.async;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesis;

import com.amazonaws.services.kinesis.model.GetRecordsResult;

import com.amazonaws.services.kinesis.model.transform.PutRecordsRequestEntryJsonUnmarshaller;

import com.amazonaws.services.kinesis.model.transform.PutRecordsRequestEntryMarshaller;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kinesis.async.testutils.KinesaliteContainer;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;
import software.amazon.awssdk.utils.AttributeMap;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** IT cases for using Kinesis consumer/producer based on Kinesalite. */
public class KinesisDataStreamsSinkITCase extends TestLogger {

    static {
        //for localhost testing only
        javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
                new javax.net.ssl.HostnameVerifier(){

                    public boolean verify(String hostname,
                                          javax.net.ssl.SSLSession sslSession) {
                        return hostname.equals("localhost");
                    }
                });
    }

    public static final String TEST_STREAM = "test_stream";

    private final ElementConverter<String, PutRecordsRequestEntry> elementConverter =
            (element, context) ->
                    PutRecordsRequestEntry.builder()
                            .data(SdkBytes.fromUtf8String(element))
                            .partitionKey(String.valueOf(element.hashCode()))
                            .build();

//    @ClassRule
//    public static MiniClusterWithClientResource miniCluster =
//            new MiniClusterWithClientResource(
//                    new MiniClusterResourceConfiguration.Builder().build());

    @ClassRule
    public static KinesaliteContainer kinesalite =
            new KinesaliteContainer(DockerImageName.parse(DockerImageVersions.KINESALITE));

    @Rule public TemporaryFolder temp = new TemporaryFolder();

    private static final SimpleStringSchema STRING_SCHEMA = new SimpleStringSchema();

//    private KinesisPubsubClient client;
//
//    @Before
//    public void setupClient() {
//        client = new KinesisPubsubClient(kinesalite.getContainerProperties());
//    }

    /**
     * Tests that pending elements do not cause a deadlock during stop with savepoint (FLINK-17170).
     *
     * <ol>
     *   <li>The test setups up a stream with 100 records and creates a Flink job that reads them
     *       with very slowly (using up a large chunk of time of the mailbox).
     *   <li>After ensuring that consumption has started, the job is stopped in a parallel thread.
     *   <li>Without the fix of FLINK-17170, the job now has a high chance to deadlock during
     *       cancel.
     *   <li>With the fix, the job proceeds and we can lift the backpressure.
     * </ol>
     */
    @Test
    public void testStopWithSavepoint() throws Exception {
//        client.createTopic(TEST_STREAM, 1, new Properties());
//
//        // add elements to the test stream
//        int numElements = 1000;
//        client.sendMessage(
//                TEST_STREAM,
//                IntStream.range(0, numElements).mapToObj(String::valueOf).toArray(String[]::new));

        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        Properties config = kinesalite.getContainerProperties();
//        config.setProperty(STREAM_INITIAL_POSITION, InitialPosition.TRIM_HORIZON.name());
//        FlinkKinesisConsumer<String> consumer =
//                new FlinkKinesisConsumer<>(TEST_STREAM, STRING_SCHEMA, config);



        KinesisAsyncClient kiness = kinesalite.getNewClient();
        kiness.createStream(CreateStreamRequest.builder().streamName("py-output").shardCount(1).build()).get();
        DescribeStreamResponse res = kiness.describeStream(DescribeStreamRequest.builder().streamName("py-output").build()).get();
        System.out.println(res);
        while(res.streamDescription().streamStatus() != StreamStatus.ACTIVE){
            res = kiness.describeStream(DescribeStreamRequest.builder().streamName("py-output").build()).get();
        }
        System.out.println(res);


        DataStream<String> stream = env.addSource(new ExampleSource());
        //.map(new WaitingMapper());
        // call stop with savepoint in another thread
//        ForkJoinTask<Object> stopTask =
//                ForkJoinPool.commonPool()
//                        .submit(
//                                () -> {
//                                    WaitingMapper.firstElement.await();
//                                    stopWithSavepoint();
//                                    WaitingMapper.stopped = true;
//                                    return null;
//                                });
        KinesisDataStreamsSinkBuilder<String> kdsSinkBuilder = KinesisDataStreamsSink.builder();
        KinesisDataStreamsSink<String> kdsSink =
                kdsSinkBuilder
                        .setElementConverter(elementConverter)
                        .setMaxTimeInBufferMS(10000)
                        .setFlushOnBufferSizeInBytes(1024)
                        .setMaxInFlightRequests(1)
                        .setMaxBatchSize(100)
                        .setMaxBufferedRequests(1000)
                        .lol(kiness)
                        .build();
        stream.sinkTo(kdsSink);
        env.execute("KDS Async Sink Example Program");
//        PutRecordsRequestEntry requestEntries = x();
//        PutRecordsRequest batchRequest =
//                PutRecordsRequest.builder().records(requestEntries).streamName("py-output").build();
//
//
//
//        PutRecordRequest req = PutRecordRequest
//                .builder()
//                .partitionKey("1")
//                .streamName("py-output")
//                .data(SdkBytes.fromUtf8String("{\"a\":\"a\",\"b\":\"a\"}"))
//                .build();
//        System.out.println(req);
//        kiness.putRecord(req).get();

        System.out.println(kiness.listShards(ListShardsRequest.builder().streamName("py-output").build()).get().shards().stream().map(x -> x.toString()).collect(
                Collectors.toList()));

        //Thread.sleep(1000000);

        String shardIterator = kiness.getShardIterator(GetShardIteratorRequest.builder().shardId("shardId-000000000000").shardIteratorType(
                ShardIteratorType.TRIM_HORIZON).streamName("py-output").build()).get().shardIterator();

        assertEquals(1000,
                kiness.getRecords(
                        GetRecordsRequest.builder().shardIterator(shardIterator).limit(1).build()).get().records().size());
//        try {
//            List<String> result = stream.executeAndCollect(10000);
//            // stop with savepoint will most likely only return a small subset of the elements
//            // validate that the prefix is as expected
//            assertThat(result, hasSize(lessThan(numElements)));
//            assertThat(
//                    result,
//                    equalTo(
//                            IntStream.range(0, numElements)
//                                    .mapToObj(String::valueOf)
//                                    .collect(Collectors.toList())
//                                    .subList(0, result.size())));
//        } finally {
//            stopTask.cancel(true);
//        }
    }

    private PutRecordsRequestEntry x(){
        String element = "hello";
        return PutRecordsRequestEntry.builder()
                .data(SdkBytes.fromUtf8String(element))
                .partitionKey(String.valueOf(element.hashCode()))
                .build();
    }

//    private String stopWithSavepoint() throws Exception {
//        JobStatusMessage job =
//                miniCluster.getClusterClient().listJobs().get().stream().findFirst().get();
//        return miniCluster
//                .getClusterClient()
//                .stopWithSavepoint(job.getJobId(), true, temp.getRoot().getAbsolutePath())
//                .get();
//    }

    private static class WaitingMapper implements MapFunction<String, String> {
        static CountDownLatch firstElement;
        static volatile boolean stopped;

        WaitingMapper() {
            firstElement = new CountDownLatch(1);
            stopped = false;
        }

        @Override
        public String map(String value) throws Exception {
            if (firstElement.getCount() > 0) {
                firstElement.countDown();
            }
            if (!stopped) {
                Thread.sleep(100);
            }
            return value;
        }
    }

    public static class ExampleSource extends RichSourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;
        private int emittedCount = 0;

        public void run(SourceContext<String> ctx) throws Exception {
            for (; this.running; Thread.sleep(5L)) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(
                            "{\"time\":" + this.emittedCount + ",\"woo\":45}");
                }

                if (this.emittedCount < 1000) {
                    ++this.emittedCount;
                } else {
                    return;
                }
            }
        }

        public void cancel() {
            this.running = false;
        }
    }
}
