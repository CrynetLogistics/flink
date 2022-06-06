package org.apache.flink.connector.aws.testutils;

import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.iam.IamAsyncClient;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createConfig;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createIAMRole;
import static org.apache.flink.connector.aws.testutils.AWSServicesTestUtils.createIamClient;

public class IamSink<InputT> extends AsyncSinkBase<InputT, String> {

    private final String customEndpoint;
    private static final int maxBatchSize = 1;
    private static final int maxInFlightRequests = 1;
    private static final int maxBufferedRequests = 10;
    private static final int maxBatchSizeInBytes = 1;
    private static final int maxTimeInBufferMS = 1;
    private static final int maxRecordSizeInBytes = 1;

    public IamSink(String customEndpoint) {
        super(
                (x, y) -> x.toString(),
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.customEndpoint = customEndpoint;
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<String>> createWriter(
            InitContext initContext) {
        return new IamSinkWriter<>(getElementConverter(), initContext);
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<String>> restoreWriter(
            InitContext initContext, Collection<BufferedRequestState<String>> collection) {
        return createWriter(initContext);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<String>> getWriterStateSerializer() {
        return new AsyncSinkWriterStateSerializer<String>() {
            @Override
            protected void serializeRequestToStream(String request, DataOutputStream out)
                    throws IOException {
                out.write(request.getBytes(StandardCharsets.UTF_8));
            }

            @Override
            protected String deserializeRequestFromStream(long requestSize, DataInputStream in)
                    throws IOException {
                byte[] requestData = new byte[(int) requestSize];
                in.read(requestData);
                return new String(requestData, StandardCharsets.UTF_8);
            }

            @Override
            public int getVersion() {
                return 0;
            }
        };
    }

    class IamSinkWriter<InputT> extends AsyncSinkWriter<InputT, String> {

        private final IamAsyncClient iamAsyncClient;

        public IamSinkWriter(
                ElementConverter<InputT, String> elementConverter, InitContext context) {
            super(
                    elementConverter,
                    context,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    maxBatchSizeInBytes,
                    maxTimeInBufferMS,
                    maxRecordSizeInBytes);
            SdkAsyncHttpClient httpClient =
                    AWSGeneralUtil.createAsyncHttpClient(
                            createConfig(customEndpoint), context.getUserCodeClassLoader());
            iamAsyncClient = createIamClient(customEndpoint, httpClient);
        }

        @Override
        protected void submitRequestEntries(
                List<String> requestEntries, Consumer<List<String>> requestResult) {
            try {
                for (String requestEntry : requestEntries) {
                    createIAMRole(iamAsyncClient, requestEntry);
                }
            } catch (ExecutionException | InterruptedException e) {
                getFatalExceptionCons().accept(e);
            }
            requestResult.accept(new ArrayList<>());
        }

        @Override
        protected long getSizeInBytes(String requestEntry) {
            return 0;
        }
    }
}
