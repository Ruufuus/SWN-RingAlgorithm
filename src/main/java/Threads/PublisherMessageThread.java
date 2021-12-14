package Threads;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class PublisherMessageThread implements Runnable {

    private final String logTag;
    private final List<String> messageBuffer = new ArrayList<>();
    private ZMQ.Socket publisherSocket;

    public PublisherMessageThread(ZMQ.Context context, String publisherAddress, String logTag) {
        this.logTag = logTag;
        createPublisherSocket(context, publisherAddress);
        new Thread(this).start();
    }

    public void sendMessage(String message) {
        synchronized (messageBuffer) {
            log.debug(logTag + "Add message to buffer:\t" + message);
            messageBuffer.add(message);
            messageBuffer.notifyAll();
        }
    }

    private void createPublisherSocket(ZMQ.Context context, String publisherAddress) {
        this.publisherSocket = context.socket(SocketType.PUB);
        publisherSocket.bind("tcp://" + publisherAddress);
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.debug(logTag + "Created Publisher socket with address:\t" + publisherAddress);
    }

    @Override
    public void run() {
        log.debug(logTag + "Publisher Thread Created!");
        while (!Thread.currentThread().isInterrupted()) {
            handleMessagesToSend();
        }
        this.publisherSocket.close();
    }

    private void handleMessagesToSend() {
        synchronized (this.messageBuffer) {
            while (this.messageBuffer.size() == 0) {
                try {
                    log.debug(logTag + "Waiting for message to send!");
                    this.messageBuffer.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            for (String message :
                    this.messageBuffer) {
                this.publisherSocket.send(message);
                log.debug(logTag + "Send message:\t" + message);
            }
            this.messageBuffer.clear();
        }
    }
}
