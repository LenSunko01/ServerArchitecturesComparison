package ru.hse.servers;

import message.proto.ClientMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;

public class NonBlockingClient extends Client {
    private final Statistics statistics;

    public NonBlockingClient(int numberOfElements, int timeBetweenMessagesInMilliSeconds,
                             int numberOfMessages, String serverHost, int serverPort,
                             ClientRunner clientRunner, Statistics statistics) {
        super(numberOfElements, timeBetweenMessagesInMilliSeconds, numberOfMessages, serverHost, serverPort, clientRunner);
        this.statistics = statistics;
    }

    public static class ClientTask extends TimerTask {
        private final Timer timer;
        private final int numberOfMessages;
        private final SocketChannel socketChannel;
        private final ClientRunner clientRunner;
        private int counter;
        private final List<ArrayList<Integer>> list;
        private final int timeBetweenMessagesInMilliSeconds;
        private final Statistics statistics;

        public ClientTask(
                List<ArrayList<Integer>> list,
                Timer timer,
                int numberOfMessages,
                SocketChannel socketChannel,
                ClientRunner clientRunner,
                int timeBetweenMessagesInMilliSeconds,
                int counter,
                Statistics statistics
        ) {
            this.list = list;
            this.timer = timer;
            this.numberOfMessages = numberOfMessages;
            this.socketChannel = socketChannel;
            this.clientRunner = clientRunner;
            this.timeBetweenMessagesInMilliSeconds = timeBetweenMessagesInMilliSeconds;
            this.counter = counter;
            this.statistics = statistics;
        }

        @Override
        public void run() {
            if (counter == numberOfMessages) {
                timer.cancel();
                try {
                    socketChannel.close();
                    clientRunner.onClientFinished();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return;
            }
            counter++;
            var requestMessage = ClientMessage
                    .newBuilder()
                    .setN(list.size())
                    .addAllElements(list.get(counter - 1)).build();
            try {
                var startTime = System.nanoTime();

                var messageLength = requestMessage.getSerializedSize();
                var writeBytes = 0;
                var sizeBuffer = ByteBuffer.allocate(4).putInt(messageLength).flip();
                while (writeBytes < 4) {
                    writeBytes += socketChannel.write(sizeBuffer);
                }
                writeBytes = 0;
                var messageBuffer = ByteBuffer.wrap(requestMessage.toByteArray());
                while (writeBytes < messageLength) {
                    writeBytes += socketChannel.write(messageBuffer);
                }

                var replySizeBuffer = ByteBuffer.allocate(4);
                var replySizeBytes = 0;
                while (replySizeBytes < 4) {
                    replySizeBytes += socketChannel.read(replySizeBuffer);
                }
                replySizeBuffer.flip();
                var size = replySizeBuffer.getInt();
                var currentMessageBytes = ByteBuffer.allocate(size);

                var readBytes = 0;
                var byteArray = new ByteArrayOutputStream();
                while (readBytes < size) {
                    readBytes += socketChannel.read(currentMessageBytes);
                    byteArray.write(currentMessageBytes.array());
                }
                var message = ClientMessage.parseFrom(byteArray.toByteArray());

                var finishTime = System.nanoTime();
                if (clientRunner.checkClientsForStatistics()) {
                    statistics.addClientTime((finishTime - startTime) / 1000000.0);
                }

                if (!checkIsSorted(list.get(counter - 1), message.getElementsList())) {
                    throw new RuntimeException("Wrong result from server");
                }

                timer.schedule(new ClientTask(list, timer, numberOfMessages, socketChannel, clientRunner, timeBetweenMessagesInMilliSeconds, counter, statistics), timeBetweenMessagesInMilliSeconds);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        List<ArrayList<Integer>> list = new ArrayList<>(numberOfMessages);
        for (var i = 0; i < numberOfMessages; i++) {
            list.add(generateArray(numberOfElements));
        }
        SocketChannel socketChannel = null;
        try {
            var timer = new Timer();
            socketChannel = SocketChannel.open(new InetSocketAddress(serverHost, serverPort));
            timer.schedule(new ClientTask(list, timer, numberOfMessages, socketChannel, clientRunner,
                    timeBetweenMessagesInMilliSeconds, 0, statistics), 0);
        } catch (Exception e) {
            e.printStackTrace();
            if (socketChannel != null) {
                try {
                    socketChannel.close();
                } catch (IOException exc) {
                    exc.printStackTrace();
                }
            }
        }
    }
}
