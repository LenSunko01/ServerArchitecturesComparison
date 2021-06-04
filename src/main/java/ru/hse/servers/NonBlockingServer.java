package ru.hse.servers;

import com.google.protobuf.InvalidProtocolBufferException;
import message.proto.ClientMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NonBlockingServer extends Server {
    private final int port;
    private volatile boolean isWorking = true;
    private Thread serverThread;

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

    private final ConcurrentHashMap<SocketChannel, ByteBuffer> readyRequests = new ConcurrentHashMap<>();

    private Selector selectorInput;
    private Selector selectorOutput;
    private ServerSocketChannel channel;

    public void start() {
        serverThread = new Thread(() -> {
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
        private final ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        private ByteBuffer currentMessageBytes = null;
        private ByteBuffer replyMessageBytes = null;

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
                currentMessageBytes = null;
                taskPool.submit(() -> {
                    var list = message.getElementsList();
                    var result = bubbleSort(new ArrayList<>(list));
                    var replyMessage = ClientMessage

                            .newBuilder()
                            .setN(result.size())
                            .addAllElements(result).build();
                    var replyMessageBytes = ByteBuffer.allocate(4 + replyMessage.getSerializedSize());
                    replyMessageBytes.putInt(replyMessage.getSerializedSize());
                    replyMessageBytes.put(replyMessage.toByteArray()).flip();
                    readyRequests.compute(clientChannel, (key, prevVal) -> {
                        if (prevVal == null) {
                            return replyMessageBytes;
                        } else {
                            ByteArrayOutputStream output = new ByteArrayOutputStream();
                            try {
                                output.write(prevVal.array());
                                output.write(replyMessageBytes.array());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            return ByteBuffer.wrap(output.toByteArray());
                        }
                    });
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
                clientChannel.read(sizeBuffer);
                if (sizeBuffer.position() == 4) {
                    byte[] tmpByteArray = sizeBuffer.array();
                    var size = ByteBuffer.wrap(tmpByteArray).getInt();
                    currentMessageBytes = ByteBuffer.allocate(size);
                    sizeBuffer.clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleAccept(SelectionKey key) throws IOException {
        SocketChannel client = channel.accept();
        client.configureBlocking(false);
        var clientData = new ClientData(client);

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
                        handleAccept(key);
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
        } catch (IOException e) {
            e.printStackTrace();
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
                        var replyMessage = (ByteBuffer) key.attachment();
                        clientChannel.write(replyMessage);
                        if (replyMessage.remaining() == 0) {
                            key.cancel();
                        }
                    }
                    it.remove();
                }
                for (var i = readyRequests.entrySet().iterator(); i.hasNext();) {
                    var entry = i.next();
                    var clientChannel = entry.getKey();
                    clientChannel.register(selectorOutput, SelectionKey.OP_WRITE, entry.getValue());
                    i.remove();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        isWorking = false;
        serverInputClientService.shutdown();
        taskPool.shutdown();
        serverThread.interrupt();
    }
}
