package ru.hse.servers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import static ru.hse.servers.Constants.*;

public class ClientRunner {
    private final int numberOfClients;
    private final int numberOfElements;
    private final int numberOfMessages;
    private final int timeBetweenMessagesInMilliseconds;
    private int finishedClients;
    private final String serverHost;
    private final int serverPort;
    private final String clientType;
    private final Server server;
    private int numberOfThreads;
    private final Statistics statistics;

    public ClientRunner(
            int numberOfClients,
            int numberOfElements,
            int timeBetweenMessagesInMilliseconds,
            int numberOfMessages,
            String serverHost,
            int serverPort,
            String clientType,
            Server server,
            int numberOfThreads,
            Statistics statistics) {
        this.numberOfClients = numberOfClients;
        this.numberOfElements = numberOfElements;
        this.timeBetweenMessagesInMilliseconds = timeBetweenMessagesInMilliseconds;
        this.numberOfMessages = numberOfMessages;
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.clientType = clientType;
        this.server = server;
        this.numberOfThreads = numberOfThreads;
        this.statistics = statistics;
    }

    public synchronized void onClientFinished() {
        finishedClients++;

        if (!checkClientsForStatistics()) {
            server.finishStatistics();
        }

        if (finishedClients == numberOfClients) {
            try (FileWriter serverWriter = new FileWriter(System.getProperty("user.dir") + File.separator + "server.txt", true);
                 FileWriter clientWriter = new FileWriter(System.getProperty("user.dir") + File.separator + "client.txt", true)) {
                serverWriter.append(String.valueOf(statistics.getServerMean()));
                serverWriter.append('\n');

                clientWriter.append(String.valueOf(statistics.getClientMean()));
                clientWriter.append('\n');
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println(statistics.getServerMean() + " " + statistics.getClientMean());
            server.stop();
        }
    }

    public void run() {
        var list = new ArrayList<Thread>();
        Client client;
        if (clientType.equals(BLOCKING_TYPE)) {
            client = new BlockingClient(numberOfElements, timeBetweenMessagesInMilliseconds,
                    numberOfMessages, serverHost, serverPort, this, statistics);
        } else if (clientType.equals(NON_BLOCKING_TYPE) || clientType.equals(ASYNCHRONOUS_TYPE)) {
            client = new NonBlockingClient(numberOfElements, timeBetweenMessagesInMilliseconds,
                    numberOfMessages, serverHost, serverPort, this, statistics);
        } else {
            throw new RuntimeException("Unexpected client type.");
        }
        for (var i = 0; i < numberOfClients; i++) {
            var clientThread = new Thread(client::run);
            clientThread.start();
            list.add(clientThread);
        }
    }

    public boolean checkClientsForStatistics() {
        return numberOfClients - finishedClients >= numberOfThreads;
    }
}
