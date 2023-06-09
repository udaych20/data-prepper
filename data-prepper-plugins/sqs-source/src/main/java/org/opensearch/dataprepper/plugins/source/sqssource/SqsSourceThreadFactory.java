package org.opensearch.dataprepper.plugins.source.sqssource;

import java.util.concurrent.ThreadFactory;

public class SqsSourceThreadFactory implements ThreadFactory {

    private int counter = 1;

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r,"sqs-thread-no: " + counter);
        counter++;
        return null;
    }
}
