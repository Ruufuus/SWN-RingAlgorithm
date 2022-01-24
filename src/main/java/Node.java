import Threads.PublisherMessageThread;
import Threads.RetransmissionThread;
import Threads.SubscriberMessageThread;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.ZMQ;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;

@Slf4j
public class Node implements Runnable {

    private final SubscriberMessageThread subscriberMessageThread;
    private final PublisherMessageThread publisherMessageThread;
    private final String logTag;
    private long tokenId = 0;
    private Long processingTime;
    private Long retransmissionTimeout;
    private boolean initializeToken;

    public Node(String publisherAddress, String subscriberAddress, boolean initializeToken) throws IOException {
        logTag = publisherAddress + "\t|\t";
        log.info(logTag + "Created node with:\n\t\t\t\t\t\tpublisher Address:\t"
                + publisherAddress + "\n\t\t\t\t\t\tsubscriber Address:\t" + subscriberAddress);
        ZMQ.Context context = ZMQ.context(1);
        this.initializeToken = initializeToken;
        subscriberMessageThread = new SubscriberMessageThread(context, subscriberAddress, logTag);
        publisherMessageThread = new PublisherMessageThread(context, publisherAddress, logTag);
        handleProperties();
        new Thread(this).start();
    }

    private void handleProperties() throws IOException {
        Properties nodeProps = new Properties();
        nodeProps.load(new FileInputStream(Objects.requireNonNull(Thread.currentThread()
                .getContextClassLoader()
                .getResource("config.properties"))
                .getPath()));
        processingTime = Long.valueOf(nodeProps.getProperty("processingTime"));
        retransmissionTimeout = Long.valueOf(nodeProps.getProperty("retransmissionTimeout"));
    }

    @Override
    public void run() {
        log.info(logTag + "Node thread created !");
        long receivedTokenId = 0;
        ArrayList<RetransmissionThread> retransmissionThreads = new ArrayList<>();
        try {
            while (!Thread.currentThread().isInterrupted()) {
                handleIfTokenIsThere(receivedTokenId, retransmissionThreads);
                receivedTokenId = Long.parseLong(subscriberMessageThread.readMessage());
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void handleIfTokenIsThere(long receivedTokenId,
                                      ArrayList<RetransmissionThread> retransmissionThreads) throws InterruptedException {
        if (initializeToken || receivedTokenId > this.tokenId) {
            log.info(logTag + "Received new token with id = " + receivedTokenId);
            tokenId = receivedTokenId + 1;
            initializeToken = false;
            log.info(logTag + "Entering Critical Section!");
            for (RetransmissionThread retransmissionThread:
                    retransmissionThreads) {
                retransmissionThread.turnOff();
            }
            Thread.sleep(processingTime);
            log.info(logTag + "Leaving Critical Section!");
            retransmissionThreads.clear();
            retransmissionThreads.add(new RetransmissionThread(publisherMessageThread,
                    retransmissionTimeout, String.valueOf(tokenId), logTag));
        }
    }
}
