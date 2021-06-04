package ru.hse.servers;

import message.proto.ClientMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class AsynchronousServer extends Server {
    private final int port;
    private final ConcurrentHashMap<SocketAddress, ByteArrayOutputStream> clientBytes = new ConcurrentHashMap<>();
    private final ExecutorService pool;
    private final AtomicInteger messagesSent = new AtomicInteger();
    private final int expectedNumberOfMessages;
    private AsynchronousServerSocketChannel serverSocket;
    private Thread serverThread;

    public AsynchronousServer(
            int port,
            int numberOfMessagesFromClient,
            int numberOfThreads,
            int numberOfClients,
            Statistics statistics) {
        super(statistics);
        this.port = port;
        expectedNumberOfMessages = numberOfClients * numberOfMessagesFromClient;
        pool = Executors.newFixedThreadPool(numberOfThreads);
        messagesSent.set(0);
    }

    public void stop() {
        pool.shutdown();
        try {
            serverSocket.close();
            serverThread.interrupt();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void writeMessageToClient(
            AsynchronousSocketChannel socketChannel,
            final ByteBuffer buf
    ) {
        socketChannel.write(buf, socketChannel, new CompletionHandler<>() {
            @Override
            public void completed(Integer result, AsynchronousSocketChannel channel) { }

            @Override
            public void failed(Throwable e, AsynchronousSocketChannel channel) {
                e.printStackTrace();
            }
        });
    }

    private void readMessageFromClient(AsynchronousSocketChannel socketChannel) {
        var buffer = ByteBuffer.allocate(1024);
        socketChannel.read(buffer, socketChannel, new CompletionHandler<>() {
            @Override
            public void completed(Integer readByteNumber, AsynchronousSocketChannel channel) {
                try {
                    if (readByteNumber == -1) {
                        return;
                    }
                    SocketAddress clientAddress = socketChannel.getRemoteAddress();
                    if (!clientBytes.containsKey(clientAddress)) {
                        clientBytes.put(clientAddress, new ByteArrayOutputStream());
                    }
                    clientBytes.get(clientAddress).write(buffer.array(), 0, readByteNumber);

                    var currentBytes = clientBytes.get(clientAddress);
                    if (currentBytes.size() >= 4) {
                        var intBuffer = currentBytes.toByteArray();
                        var sizeBytes = Arrays.copyOfRange(intBuffer, 0, 4);
                        var size = ByteBuffer.wrap(sizeBytes).getInt();
                        if (currentBytes.size() >= 4 + size) {
                            var message = ClientMessage.parseFrom(Arrays.copyOfRange(intBuffer, 4, 4 + size));
                            var newByteArray = new ByteArrayOutputStream();
                            newByteArray.write(Arrays.copyOfRange(intBuffer, 4 + size, intBuffer.length));
                            clientBytes.put(clientAddress, newByteArray);

                            pool.submit(() -> {
                                var list = message.getElementsList();
                                var result = bubbleSort(new ArrayList<>(list));
                                var replyMessage = ClientMessage
                                        .newBuilder()
                                        .setN(result.size())
                                        .addAllElements(result).build();
                                var replyBuffer = ByteBuffer.allocate(4 + replyMessage.getSerializedSize())
                                        .putInt(replyMessage.getSerializedSize())
                                        .put(replyMessage.toByteArray())
                                        .flip();
                                writeMessageToClient(socketChannel, replyBuffer);
                            });
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
                readMessageFromClient(channel);
            }

            @Override
            public void failed(Throwable e, AsynchronousSocketChannel channel) {
                e.printStackTrace();
            }
        });
    }

    public void start() {
        serverThread = new Thread(() -> {
            var socketAddress = new InetSocketAddress("localhost", port);
            try {
                serverSocket = AsynchronousServerSocketChannel.open();
                serverSocket.bind(socketAddress);
                serverSocket.accept(serverSocket, new CompletionHandler<>() {
                    @Override
                    public void completed(AsynchronousSocketChannel socketChannel, AsynchronousServerSocketChannel serverSocket) {
                        serverSocket.accept(serverSocket, this);
                        readMessageFromClient(socketChannel);
                    }

                    @Override
                    public void failed(Throwable e, AsynchronousServerSocketChannel serverSocket) {
                        e.printStackTrace();
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        serverThread.start();
    }
}
