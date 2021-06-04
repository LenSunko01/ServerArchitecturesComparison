package ru.hse.servers;

import message.proto.ClientMessage;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BlockingServer extends Server {
    private final int port;
    private final int totalMessages;
    private final int numberOfThreads;
    private final int totalNumberOfClients;
    private volatile boolean isWorking = true;
    private ExecutorService pool;
    private final ArrayList<ExecutorService> threads = new ArrayList<>();
    private Thread serverThread;

    @Override
    public void stop() {
        isWorking = false;
        pool.shutdown();
        for (var thread : threads) {
            thread.shutdown();
        }
        serverThread.interrupt();
    }

    private class TaskExecutor implements Runnable {
        private final ClientMessage message;
        private final Socket socket;
        private final ExecutorService threadOut;
        public TaskExecutor(ClientMessage message, Socket socket, ExecutorService threadOut) {
            this.message = message;
            this.socket = socket;
            this.threadOut = threadOut;
        }

        @Override
        public void run() {
            var list = message.getElementsList();
            var result = bubbleSort(new ArrayList<>(list));
            threadOut.submit(() -> {
                var replyMessage = ClientMessage
                        .newBuilder()
                        .setN(result.size())
                        .addAllElements(result).build();
                try {
                    var os = socket.getOutputStream();
                    replyMessage.writeDelimitedTo(os);
                    os.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private class ThreadReader implements Runnable {
        private final Socket socket;
        private final int totalMessages;
        private final ExecutorService threadOut;
        public ThreadReader(Socket socket, int totalMessages) {
            this.socket = socket;
            this.totalMessages = totalMessages;
            this.threadOut = Executors.newSingleThreadExecutor();
            threads.add(threadOut);
        }
        @Override
        public void run() {
            try {
                var is = socket.getInputStream();
                for (var i = 0; i < totalMessages; i++) {
                    is.readNBytes(4);
                    var message = ClientMessage.parseDelimitedFrom(is);
                    pool.submit(new TaskExecutor(message, socket, threadOut));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public BlockingServer(
            int port,
            int totalMessages,
            int numberOfThreads,
            int totalNumberOfClients,
            Statistics statistics) {
        super(statistics);
        this.port = port;
        this.totalMessages = totalMessages;
        this.numberOfThreads = numberOfThreads;
        this.totalNumberOfClients = totalNumberOfClients;
    }

    public void start() {
        serverThread = new Thread(() -> {
            try (var s = new ServerSocket(port)) {
                pool = Executors.newFixedThreadPool(numberOfThreads);
                for (var i = 0; i < totalNumberOfClients; i++) {
                    var socket = s.accept();
                    var serverThread = new Thread(new ThreadReader(socket, totalMessages));
                    serverThread.start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        serverThread.start();
    }
}
