package org.apache.flink.connector.base.sink.writer;

public class RequestEntryWrapper<RequestEntryT> {

    private final RequestEntryT requestEntry;
    private final long size;

    public RequestEntryWrapper(RequestEntryT requestEntry, long size){
        this.requestEntry = requestEntry;
        this.size = size;
    }

    public RequestEntryT getRequestEntry() {
        return requestEntry;
    }

    public long getSize() {
        return size;
    }
}
