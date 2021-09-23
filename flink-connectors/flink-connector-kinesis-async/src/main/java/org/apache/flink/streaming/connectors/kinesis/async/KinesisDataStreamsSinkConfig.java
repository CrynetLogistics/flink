package org.apache.flink.streaming.connectors.kinesis.async;

import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.io.Serializable;

/** Configuration for {@link KinesisDataStreamsSink}. */
public class KinesisDataStreamsSinkConfig<InputT> implements Serializable {

    private static final int DEFAULT_MAX_BATCH_SIZE = 100;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 8;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 1000;
    private static final long DEFAULT_FLUSH_ON_BUFFER_SIZE_IN_B = 64 * 1024 * 1024;
    private static final long DEFAULT_MAX_TimeInBufferMS = 1000;

    private final ElementConverter<InputT, PutRecordsRequestEntry> elementConverter;
    private final int maxBatchSize;
    private final int maxInFlightRequests;
    private final int maxBufferedRequests;
    private final long flushOnBufferSizeInBytes;
    private final long maxTimeInBufferMS;
    private final String streamName;

    public KinesisDataStreamsSinkConfig(ElementConverter<InputT, PutRecordsRequestEntry> elementConverter,
                                        int maxBatchSize,
                                        int maxInFlightRequests,
                                        int maxBufferedRequests,
                                        long flushOnBufferSizeInBytes,
                                        long maxTimeInBufferMS,
                                        String streamName) {
        this.elementConverter = elementConverter;
        this.maxBatchSize = maxBatchSize;
        this.maxInFlightRequests = maxInFlightRequests;
        this.maxBufferedRequests = maxBufferedRequests;
        this.flushOnBufferSizeInBytes = flushOnBufferSizeInBytes;
        this.maxTimeInBufferMS = maxTimeInBufferMS;
        this.streamName = streamName;
    }

    /**
     * Create a {@link Builder} to construct a new {@link
     * KinesisDataStreamsSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link Builder}
     */
    public static <InputT> Builder<InputT> builder() {
        return new Builder<>();
    }

    public ElementConverter<InputT, PutRecordsRequestEntry> getElementConverter() {
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

    public String getStreamName() {
        return streamName;
    }

    public static class Builder<InputT> {

        private ElementConverter<InputT, PutRecordsRequestEntry> elementConverter;
        private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
        private int maxInFlightRequests = DEFAULT_MAX_IN_FLIGHT_REQUESTS;
        private int maxBufferedRequests = DEFAULT_MAX_BUFFERED_REQUESTS;
        private long flushOnBufferSizeInBytes = DEFAULT_FLUSH_ON_BUFFER_SIZE_IN_B;
        private long maxTimeInBufferMS = DEFAULT_MAX_TimeInBufferMS;
        private String streamName;

        public Builder<InputT> setElementConverter(
                ElementConverter<InputT, PutRecordsRequestEntry> elementConverter) {
            this.elementConverter = elementConverter;
            return this;
        }

        public Builder<InputT> setMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder<InputT> setMaxInFlightRequests(int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        public Builder<InputT> setMaxBufferedRequests(int maxBufferedRequests) {
            this.maxBufferedRequests = maxBufferedRequests;
            return this;
        }

        public Builder<InputT> setFlushOnBufferSizeInBytes(
                long flushOnBufferSizeInBytes) {
            this.flushOnBufferSizeInBytes = flushOnBufferSizeInBytes;
            return this;
        }

        public Builder<InputT> setMaxTimeInBufferMS(long maxTimeInBufferMS) {
            this.maxTimeInBufferMS = maxTimeInBufferMS;
            return this;
        }

        public Builder<InputT> setStreamName(String streamName) {
            Preconditions.checkArgument(!streamName.isEmpty());
            this.streamName = streamName;
            return this;
        }

        public KinesisDataStreamsSinkConfig<InputT> build() {
            Preconditions.checkNotNull(streamName, "The stream name must be set and "
                    + "set to a non null value when initializing the KDS Sink.");
            Preconditions.checkNotNull(elementConverter, "A non-null element converter must be "
                    + "provided when initializing the KDS Sink.");
            return new KinesisDataStreamsSinkConfig<>(
                    elementConverter,
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    flushOnBufferSizeInBytes,
                    maxTimeInBufferMS,
                    streamName);
        }
    }

}
