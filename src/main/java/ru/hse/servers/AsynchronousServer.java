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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class AsynchronousServer extends Server {
    private final int port;
    private final ConcurrentHashMap<SocketAddress, ByteArrayOutputStream> clientBytesInput = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<AsynchronousSocketChannel, ConcurrentLinkedQueue<ByteBuffer>> clientBytesOutput = new ConcurrentHashMap<>();
    private final ExecutorService pool;
    private final int numberOfClients;
    private AsynchronousServerSocketChannel serverSocket;
    private Thread serverThread;
    private final AtomicInteger numberOfClientAccepted = new AtomicInteger(0);
    private final ArrayList<AsynchronousSocketChannel> clientChannels = new ArrayList<>();

    public AsynchronousServer(
            int port,
            int numberOfThreads,
            int numberOfClients,
            Statistics statistics) {
        super(statistics);
        this.port = port;
        pool = Executors.newFixedThreadPool(numberOfThreads);
        this.numberOfClients = numberOfClients;
    }

    public void stop() {
        pool.shutdown();
        for (var clientChannel : clientChannels) {
            try {
                clientChannel.shutdownInput();
                clientChannel.shutdownOutput();
                clientChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeMessageToClient(
            AsynchronousSocketChannel socketChannel,
            final ByteBuffer buf
    ) {
        synchronized(clientBytesOutput.get(socketChannel)) {
            var queue = clientBytesOutput.get(socketChannel);
            if (queue.isEmpty()) {
                queue.add(buf);
                socketChannel.write(queue.peek(), socketChannel, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer result, AsynchronousSocketChannel channel) {
                        synchronized(clientBytesOutput.get(socketChannel)) {
                            if (!queue.isEmpty() && queue.peek().remaining() == 0) {
                                queue.poll();
                            }
                            if (!queue.isEmpty()) {
                                socketChannel.write(queue.peek(), socketChannel, this);
                            }
                        }
                    }

                    @Override
                    public void failed(Throwable e, AsynchronousSocketChannel channel) {
                        e.printStackTrace();
                    }
                });
            } else {
                queue.add(buf);
            }
        }
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
                    if (!clientBytesInput.containsKey(clientAddress)) {
                        clientBytesInput.put(clientAddress, new ByteArrayOutputStream());
                    }
                    clientBytesInput.get(clientAddress).write(buffer.array(), 0, readByteNumber);

                    var currentBytes = clientBytesInput.get(clientAddress);
                    if (currentBytes.size() >= 8) {
                        var intBuffer = currentBytes.toByteArray();
                        var sizeBytes = Arrays.copyOfRange(intBuffer, 0, 4);
                        var size = ByteBuffer.wrap(sizeBytes).getInt();
                        var messageOrderBytes = Arrays.copyOfRange(intBuffer, 4, 8);
                        var messageOrder = ByteBuffer.wrap(messageOrderBytes).getInt();
                        if (currentBytes.size() >= 8 + size) {
                            var message = ClientMessage.parseFrom(Arrays.copyOfRange(intBuffer, 8, 8 + size));
                            var newByteArray = new ByteArrayOutputStream();
                            newByteArray.write(Arrays.copyOfRange(intBuffer, 8 + size, intBuffer.length));
                            clientBytesInput.put(clientAddress, newByteArray);

                            pool.submit(() -> {
                                var list = message.getElementsList();
                                var result = bubbleSort(new ArrayList<>(list));
                                var replyMessage = ClientMessage
                                        .newBuilder()
                                        .setN(result.size())
                                        .addAllElements(result).build();
                                var replyBuffer = ByteBuffer.allocate(8 + replyMessage.getSerializedSize())
                                        .putInt(replyMessage.getSerializedSize())
                                        .putInt(messageOrder)
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
                        clientChannels.add(socketChannel);
                        clientBytesOutput.put(socketChannel, new ConcurrentLinkedQueue<>());
                        if (numberOfClients > numberOfClientAccepted.incrementAndGet()) {
                            serverSocket.accept(serverSocket, this);
                        }
                        readMessageFromClient(socketChannel);
                    }

                    @Override
                    public void failed(Throwable e, AsynchronousServerSocketChannel serverSocket) {
                        e.printStackTrace();
                    }
                });
            } catch (IOException e) {
                try {
                    serverSocket.close();
                } catch (IOException exc) {
                    exc.printStackTrace();
                }
                e.printStackTrace();
            }
        });
        serverThread.start();
    }
}
