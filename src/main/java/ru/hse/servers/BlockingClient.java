package ru.hse.servers;

import message.proto.ClientMessage;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingClient extends Client {
    private final Statistics statistics;

    public BlockingClient(
            int numberOfElements,
            int timeBetweenMessagesInMilliSeconds,
            int numberOfMessages,
            String serverHost,
            int serverPort,
            ClientRunner clientRunner,
            Statistics statistics
    ) {
        super(numberOfElements, timeBetweenMessagesInMilliSeconds, numberOfMessages,
                serverHost, serverPort, clientRunner);
        this.statistics = statistics;
    }

    public static class ClientTask extends TimerTask {
        private final OutputStream outputStream;
        private final Timer timer;
        private final AtomicInteger counter = new AtomicInteger(0);
        private final int numberOfMessages;
        private final List<ArrayList<Integer>> list;
        private final ConcurrentHashMap<Integer, Long> times;
        private final AtomicBoolean messageIsSent;

        public ClientTask(
                OutputStream outputStream,
                List<ArrayList<Integer>> list,
                Timer timer,
                int numberOfMessages,
                ConcurrentHashMap<Integer, Long> times,
                AtomicBoolean messageIsSent
        ) {
            this.list = list;
            this.outputStream = outputStream;
            this.timer = timer;
            this.numberOfMessages = numberOfMessages;
            this.times = times;
            this.messageIsSent = messageIsSent;
        }

        @Override
        public void run() {
            if (counter.get() == numberOfMessages) {
                timer.cancel();
                return;
            }
            var currentMessage = counter.incrementAndGet();
            var requestMessage = ClientMessage
                    .newBuilder()
                    .setN(list.size())
                    .addAllElements(list.get(currentMessage - 1)).build();
            try {
                while (!messageIsSent.compareAndSet(true, false)) ;
                var startTime = System.nanoTime();
                times.put(currentMessage, startTime);
                var dataStream = new DataOutputStream(outputStream);
                dataStream.writeInt(currentMessage);
                requestMessage.writeDelimitedTo(dataStream);
                messageIsSent.set(true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void run() {
        List<ArrayList<Integer>> list = new ArrayList<>(numberOfMessages);
        for (var i = 0; i < numberOfMessages; i++) {
            list.add(generateArray(numberOfElements));
        }
        try (Socket s = new Socket(serverHost, serverPort)) {
            var is = s.getInputStream();
            var os = s.getOutputStream();
            var timer = new Timer();
            var times = new ConcurrentHashMap<Integer, Long>();
            var messageIsSent = new AtomicBoolean(true);
            timer.scheduleAtFixedRate(new ClientTask(os, list, timer, numberOfMessages,
                    times, messageIsSent), 0, timeBetweenMessagesInMilliSeconds);
            int numberOfMessagesParsed = 0;
            while (numberOfMessagesParsed < numberOfMessages) {
                var messageOrder = ByteBuffer.wrap(is.readNBytes(4)).getInt();
                var message = ClientMessage.parseDelimitedFrom(is);

                var finishTime = System.nanoTime();
                if (clientRunner.checkClientsForStatistics()) {
                    statistics.addClientTime((finishTime - times.get(messageOrder)) / 1000000.0);
                }

                if (!checkIsSorted(list.get(messageOrder - 1), message.getElementsList())) {
                    throw new RuntimeException("Wrong result from server");
                }
                numberOfMessagesParsed++;
            }
            clientRunner.onClientFinished();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
