package ru.hse.servers;

import message.proto.ClientMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;

public class BlockingClient extends Client {
    private final Statistics statistics;

    public BlockingClient(int numberOfElements, int timeBetweenMessagesInMilliSeconds, int numberOfMessages, String serverHost, int serverPort, ClientRunner clientRunner, Statistics statistics) {
        super(numberOfElements, timeBetweenMessagesInMilliSeconds, numberOfMessages, serverHost, serverPort, clientRunner);
        this.statistics = statistics;
    }

    public static class ClientTask extends TimerTask {
        private final InputStream inputStream;
        private final OutputStream outputStream;
        private final Timer timer;
        private final int numberOfMessages;
        private final Socket socket;
        private final ClientRunner clientRunner;
        private int counter;
        private final List<ArrayList<Integer>> list;
        private final Statistics statistics;

        public ClientTask(
                InputStream inputStream,
                OutputStream outputStream,
                List<ArrayList<Integer>> list,
                Timer timer,
                int numberOfMessages,
                Socket socket,
                ClientRunner clientRunner, Statistics statistics) {
            this.inputStream = inputStream;
            this.list = list;
            this.outputStream = outputStream;
            this.timer = timer;
            this.numberOfMessages = numberOfMessages;
            this.socket = socket;
            this.clientRunner = clientRunner;
            this.statistics = statistics;
            counter = 0;
        }
        @Override
        public void run() {
            if (counter == numberOfMessages) {
                timer.cancel();
                try {
                    socket.close();
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

                outputStream.write(ByteBuffer.allocate(4).putInt(requestMessage.getSerializedSize()).array());
                requestMessage.writeDelimitedTo(outputStream);
                var message = ClientMessage.parseDelimitedFrom(inputStream);

                var finishTime = System.nanoTime();
                if (clientRunner.checkClientsForStatistics()) {
                    statistics.addClientTime((finishTime - startTime) / 1000000.0);
                }

                if (!checkIsSorted(list.get(counter - 1), message.getElementsList())) {
                    throw new RuntimeException("Wrong result from server");
                }
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
        try {
            var s = new Socket(serverHost, serverPort);
            var is = s.getInputStream();
            var os = s.getOutputStream();
            var timer = new Timer();
            timer.scheduleAtFixedRate(new ClientTask(is, os, list, timer, numberOfMessages, s, clientRunner, statistics), 0, timeBetweenMessagesInMilliSeconds);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
