package ru.hse.servers;

import com.google.protobuf.InvalidProtocolBufferException;
import message.proto.ClientMessage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NonBlockingServer extends Server {
    private final int port;
    private volatile boolean isWorking = true;

    public NonBlockingServer(
            int port,
            int numberOfThreads,
            Statistics statistics) {
        super(statistics);
        this.port = port;
        taskPool = Executors.newFixedThreadPool(numberOfThreads);
    }


    private final ExecutorService serverInputClientService = Executors.newSingleThreadExecutor();
    private final ExecutorService taskPool;

    private final ConcurrentHashMap<SocketChannel, ClientData> readyRequests = new ConcurrentHashMap<>();
    private final ArrayList<SocketChannel> clientsSockets = new ArrayList<>();
    private Selector selectorInput;
    private Selector selectorOutput;
    private ServerSocketChannel channel;

    public void start() {
        Thread serverThread = new Thread(() -> {
            try {
                selectorInput = Selector.open();
                selectorOutput = Selector.open();
                channel = ServerSocketChannel.open();
                var socketAddress = new InetSocketAddress("localhost", port);
                channel.bind(socketAddress);
                channel.configureBlocking(false);
                int ops = channel.validOps();

                channel.register(selectorInput, ops, null);
                serverInputClientService.submit(this::acceptRequest);
                sendReplies();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        serverThread.start();
    }

    private class ClientData {
        private final SocketChannel clientChannel;
        private final ByteBuffer headerBuffer = ByteBuffer.allocate(8);
        private ByteBuffer currentMessageBytes = null;
        private int currentMessageOrder;
        private final ConcurrentLinkedQueue<ByteBuffer> messages = new ConcurrentLinkedQueue<>();


        private ClientData(SocketChannel clientChannel) {
            this.clientChannel = clientChannel;
        }

        public boolean messageIsReady() {
            if (currentMessageBytes == null) {
                return false;
            }
            return (currentMessageBytes.position() == currentMessageBytes.capacity());
        }

        public void processMessage() {
            try {
                var message = ClientMessage.parseFrom(currentMessageBytes.array());
                var order = currentMessageOrder;
                currentMessageBytes = null;
                taskPool.submit(() -> {
                    var list = message.getElementsList();
                    var result = bubbleSort(new ArrayList<>(list));
                    var replyMessage = ClientMessage
                            .newBuilder()
                            .setN(result.size())
                            .addAllElements(result).build();
                    var replyMessageBytes = ByteBuffer.allocate(8 + replyMessage.getSerializedSize())
                            .putInt(replyMessage.getSerializedSize())
                            .putInt(order)
                            .put(replyMessage.toByteArray()).flip();
                    messages.add(replyMessageBytes);
                    readyRequests.putIfAbsent(clientChannel, this);
                    selectorOutput.wakeup();
                });
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }

        public void readRequest() {
            try {
                if (currentMessageBytes != null) {
                    clientChannel.read(currentMessageBytes);
                    return;
                }
                if (!clientChannel.isOpen()) {
                    return;
                }
                clientChannel.read(headerBuffer);
                if (headerBuffer.position() == 8) {
                    var intBuffer = headerBuffer.array();
                    var sizeBytes = Arrays.copyOfRange(intBuffer, 0, 4);
                    var size = ByteBuffer.wrap(sizeBytes).getInt();
                    var messageOrderBytes = Arrays.copyOfRange(intBuffer, 4, 8);
                    currentMessageOrder = ByteBuffer.wrap(messageOrderBytes).getInt();

                    currentMessageBytes = ByteBuffer.allocate(size);
                    headerBuffer.clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleAccept() throws IOException {
        SocketChannel client = channel.accept();
        client.configureBlocking(false);
        var clientData = new ClientData(client);
        clientsSockets.add(client);

        client.register(selectorInput, SelectionKey.OP_READ, clientData);
    }

    private void acceptRequest() {
        try {
            while (isWorking) {
                selectorInput.select();
                Set<SelectionKey> selectedKeys = selectorInput.selectedKeys();
                Iterator<SelectionKey> it = selectedKeys.iterator();

                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    if (key.isAcceptable()) {
                        handleAccept();
                    } else if (key.isReadable()) {
                        var clientData = (ClientData) key.attachment();
                        clientData.readRequest();
                        if (clientData.messageIsReady()) {
                            clientData.processMessage();
                        }
                    }
                    it.remove();
                }
            }
            selectorInput.close();
        } catch (IOException e) {
            try {
                selectorInput.close();
            } catch (IOException exc) {
                exc.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    private void register() throws ClosedChannelException {
        for (var i = readyRequests.entrySet().iterator(); i.hasNext(); ) {
            var entry = i.next();
            var clientChannel = entry.getKey();
            clientChannel.register(selectorOutput, SelectionKey.OP_WRITE, entry.getValue());
            i.remove();
        }
    }

    private void sendReplies() {
        try {
            while (isWorking) {
                selectorOutput.select();

                Set<SelectionKey> selectedKeys = selectorOutput.selectedKeys();
                Iterator<SelectionKey> it = selectedKeys.iterator();

                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    if (key.isWritable()) {
                        var clientChannel = (SocketChannel) key.channel();
                        var clientData = (ClientData) key.attachment();
                        var replyMessages = clientData.messages;
                        clientChannel.write(replyMessages.peek());
                        if (!replyMessages.isEmpty() && replyMessages.peek().remaining() == 0) {
                            replyMessages.poll();
                        }
                        if (replyMessages.isEmpty()) {
                            key.interestOps(0);
                        }
                    }
                    it.remove();
                }
                register();
            }
            selectorOutput.close();
        } catch (IOException e) {
            try {
                selectorOutput.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    public void stop() {
        isWorking = false;
        selectorOutput.wakeup();
        selectorInput.wakeup();
        serverInputClientService.shutdown();
        taskPool.shutdown();
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (var clientSocket : clientsSockets) {
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
