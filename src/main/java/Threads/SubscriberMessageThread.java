package Threads;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class SubscriberMessageThread implements Runnable {

    private final String logTag;
    private final List<String> messageBuffer = new ArrayList<>();
    private ZMQ.Socket subscriberSocket;

    public SubscriberMessageThread(ZMQ.Context context, String subscriberAddress, String logTag) {
        this.logTag = logTag;
        createSubscriberSocket(context, subscriberAddress);
        new Thread(this).start();
    }


    private void createSubscriberSocket(ZMQ.Context context, String subscriberAddress) {
        this.subscriberSocket = context.socket(SocketType.SUB);
        subscriberSocket.connect("tcp://" + subscriberAddress);
        subscriberSocket.subscribe(ZMQ.SUBSCRIPTION_ALL);
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.debug(logTag + "Created Subscriber socket with address:\t" + subscriberAddress);
    }

    public String readMessage() {
        String readMessage;
        synchronized (this.messageBuffer) {
            while (this.messageBuffer.size() == 0) {
                log.debug(logTag + "Waiting for message to read!");
                try {
                    this.messageBuffer.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            readMessage = this.messageBuffer.get(0);
            this.messageBuffer.clear();
            log.debug(logTag + "Read message from buffer:\t" + readMessage);
        }
        return readMessage;
    }


    @Override
    public void run() {
        log.debug(logTag + "Subscriber Thread Created!");
        while (!Thread.currentThread().isInterrupted()) {
            String message = this.subscriberSocket.recvStr();
            synchronized (this.messageBuffer) {
                log.debug(logTag + "Incoming message:\t" + message);
                this.messageBuffer.add(message);
                this.messageBuffer.notifyAll();
            }
        }
        this.subscriberSocket.close();
    }
}
