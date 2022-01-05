package org.apache.flink.connector.firehose.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.firehose.model.Record;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.utils.ImmutableMap;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;

public class KinesisDataFirehoseSinkITCase {

    private static final ElementConverter<String, Record> elementConverter =
            KinesisDataFirehoseSinkElementConverter.<String>builder()
                    .setSerializationSchema(new SimpleStringSchema())
                    .build();

    @BeforeEach
    public void setup(){
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
    }

    @AfterEach
    public void teardown(){
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
    }

    public static void listBucketObjects(S3AsyncClient s3, String bucketName ) {

        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            CompletableFuture<ListObjectsResponse> resC = s3.listObjects(listObjects);
            ListObjectsResponse res = resC.get();
            List<S3Object> objects = res.contents();

            for (S3Object myValue: objects ) {
                System.out.print("\n The name of the key is " + myValue.key());
                System.out.print("\n The object is " + calKb(myValue.size()) + " KBs");
                System.out.print("\n The owner is " + myValue.owner());

            }

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    //convert bytes to kbs
    private static long calKb(Long val) {
        return val/1024;
    }

    @Test
    public void test2() throws URISyntaxException {
        Properties config = new Properties();
        config.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "lol");
        config.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "lol");
        config.setProperty(TRUST_ALL_CERTIFICATES, "true");
        config.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");

        final SdkAsyncHttpClient httpClient =
                AWSGeneralUtil.createAsyncHttpClient(config);
        S3AsyncClient client = S3AsyncClient.builder().httpClient(httpClient)
                .region(Region.AP_SOUTHEAST_1).endpointOverride(new URI("https://localhost:4566")).build();

        listBucketObjects(client, "s3-firehose");
    }

    @Test
    public void test() throws Exception {

        Properties config = new Properties();
        config.setProperty(AWSConfigConstants.AWS_REGION, "ap-southeast-1");
        config.setProperty(AWSConfigConstants.AWS_ENDPOINT, "https://localhost:4566");
        config.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "lol");
        config.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "lol");
        config.setProperty(TRUST_ALL_CERTIFICATES, "true");
        config.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");


        ObjectMapper mapper = new ObjectMapper();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<String> fromGen =
                env.fromSequence(1, 10_000_000L)
                        .map(Object::toString)
                        .returns(String.class)
                        .map(data -> mapper.writeValueAsString(ImmutableMap.of("data", data)));

        KinesisDataFirehoseSink<String> kdsSink =
                KinesisDataFirehoseSink.<String>builder()
                        .setElementConverter(elementConverter)
                        .setDeliveryStreamName("s3-stream")
                        .setMaxBatchSize(20)
                        .setKinesisClientProperties(config)
                        .build();

        fromGen.sinkTo(kdsSink);

        env.execute("KDF Async Sink Example Program");

    }
}
