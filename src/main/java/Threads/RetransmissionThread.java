package Threads;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetransmissionThread implements Runnable {
    private final PublisherMessageThread publisherMessageThread;
    private final Long retransmissionTimeout;
    private final String message;
    private Boolean isAlive = Boolean.TRUE;
    private final String logTag;

    public RetransmissionThread(PublisherMessageThread publisherMessageThread,
                                Long retransmissionTimeout,
                                String message,
                                String logTag) {
        this.publisherMessageThread = publisherMessageThread;
        this.retransmissionTimeout = retransmissionTimeout;
        this.message = message;
        this.logTag = logTag;
        new Thread(this).start();
    }

    public void turnOff() {
        log.debug(logTag + "Shutting down retransmission Thread!");
        this.isAlive = false;
    }

    @Override
    public void run() {
        log.debug(logTag + "Retransmission Thread created!");
        while (isAlive || !Thread.currentThread().isInterrupted()) {
            publisherMessageThread.sendMessage(this.message);
            log.debug(logTag + "Retransmissioned message:\t" + message);
            try {
                Thread.sleep(retransmissionTimeout);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
