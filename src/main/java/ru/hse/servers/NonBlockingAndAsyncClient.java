package ru.hse.servers;

import message.proto.ClientMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NonBlockingAndAsyncClient extends Client {
    private final Statistics statistics;
    public NonBlockingAndAsyncClient(int numberOfElements, int timeBetweenMessagesInMilliSeconds,
                                     int numberOfMessages, String serverHost, int serverPort,
                                     ClientRunner clientRunner, Statistics statistics) {
        super(numberOfElements, timeBetweenMessagesInMilliSeconds, numberOfMessages,
                serverHost, serverPort, clientRunner);
        this.statistics = statistics;
    }

    public static class ClientTask extends TimerTask {
        private final Timer timer;
        private final int numberOfMessages;
        private final SocketChannel socketChannel;
        private final ClientRunner clientRunner;
        private final AtomicInteger counter;
        private final List<ArrayList<Integer>> list;
        private final int timeBetweenMessagesInMilliSeconds;
        private final Statistics statistics;
        private final ConcurrentHashMap<Integer, Long> times;
        private final AtomicBoolean messageIsSent;
        public ClientTask(
                List<ArrayList<Integer>> list,
                Timer timer,
                int numberOfMessages,
                SocketChannel socketChannel,
                ClientRunner clientRunner,
                int timeBetweenMessagesInMilliSeconds,
                AtomicInteger counter,
                Statistics statistics,
                ConcurrentHashMap<Integer, Long> times, AtomicBoolean messageIsSent) {
            this.list = list;
            this.timer = timer;
            this.numberOfMessages = numberOfMessages;
            this.socketChannel = socketChannel;
            this.clientRunner = clientRunner;
            this.timeBetweenMessagesInMilliSeconds = timeBetweenMessagesInMilliSeconds;
            this.counter = counter;
            this.statistics = statistics;
            this.times = times;
            this.messageIsSent = messageIsSent;
        }

        private void writeInt(int value) throws IOException {
            var writeBytes = 0;
            var buffer = ByteBuffer.allocate(4).putInt(value).flip();
            while (writeBytes < 4) {
                writeBytes += socketChannel.write(buffer);
            }
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
                timer.schedule(new ClientTask(list, timer, numberOfMessages, socketChannel, clientRunner,
                        timeBetweenMessagesInMilliSeconds, counter, statistics, times, messageIsSent), timeBetweenMessagesInMilliSeconds);
                while (!messageIsSent.compareAndSet(true, false));
                var startTime = System.nanoTime();
                times.put(currentMessage, startTime);
                var messageLength = requestMessage.getSerializedSize();
                writeInt(messageLength);
                writeInt(currentMessage);
                var writeBytes = 0;
                var messageBuffer = ByteBuffer.wrap(requestMessage.toByteArray());
                while (writeBytes < messageLength) {
                    writeBytes += socketChannel.write(messageBuffer);
                }
                messageIsSent.set(true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private int readInt(SocketChannel socketChannel) throws IOException {
        var buffer = ByteBuffer.allocate(4);
        var bytesRead = 0;
        while (bytesRead < 4) {
            bytesRead += socketChannel.read(buffer);
        }
        buffer.flip();
        return buffer.getInt();
    }

    @Override
    public void run() {
        List<ArrayList<Integer>> list = new ArrayList<>(numberOfMessages);
        for (var i = 0; i < numberOfMessages; i++) {
            list.add(generateArray(numberOfElements));
        }
        try (SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(serverHost, serverPort))) {
            var timer = new Timer();
            ConcurrentHashMap<Integer, Long> times = new ConcurrentHashMap<>();
            var messageIsSent = new AtomicBoolean(true);
            timer.schedule(new NonBlockingAndAsyncClient.ClientTask(list, timer, numberOfMessages, socketChannel, clientRunner,
                    timeBetweenMessagesInMilliSeconds, new AtomicInteger(0), statistics, times, messageIsSent), 0);
            int numberOfMessagesParsed = 0;
            while (numberOfMessagesParsed < numberOfMessages) {
                var size = readInt(socketChannel);
                var currentMessageBytes = ByteBuffer.allocate(size);
                var messageOrder = readInt(socketChannel);

                var readBytes = 0;
                while (readBytes < size) {
                    readBytes += socketChannel.read(currentMessageBytes);
                }
                var message = ClientMessage.parseFrom(currentMessageBytes.array());

                var finishTime = System.nanoTime();
                if (clientRunner.checkClientsForStatistics()) {
                    statistics.addClientTime((finishTime - times.get(messageOrder)) / 1000000.0);
                }

                if (!checkIsSorted(list.get(messageOrder - 1), message.getElementsList())) {
                    throw new RuntimeException("Wrong result from server");
                }
                numberOfMessagesParsed++;
            }
            try {
                socketChannel.shutdownInput();
                socketChannel.shutdownOutput();
                socketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            clientRunner.onClientFinished();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
