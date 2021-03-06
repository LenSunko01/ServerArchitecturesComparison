package ru.hse.servers;

import static ru.hse.servers.Constants.*;

public class Application {
    public static void main(String[] args) throws InterruptedException {
        var architecture = args[0];
        int numberOfMessages = Integer.parseInt(args[1]);
        int numberOfClients = Integer.parseInt(args[2]);
        var numberOfElements = Integer.parseInt(args[3]);
        var numberOfThreads = Integer.parseInt(args[4]);
        var timeBetweenMessagesMillis = Integer.parseInt(args[5]);

        runExperiment(architecture, numberOfMessages, numberOfClients, numberOfElements, numberOfThreads, timeBetweenMessagesMillis);
    }

    private static void runExperiment(String architecture, int numberOfMessages, int numberOfClients, int numberOfElements, int numberOfThreads, int timeBetweenMessagesMillis) throws InterruptedException {
        Server server;
        var statistics = new Statistics();
        switch (architecture) {
            case BLOCKING_TYPE:
                server = new BlockingServer(8080, numberOfMessages, numberOfThreads, numberOfClients, statistics);
                break;
            case NON_BLOCKING_TYPE:
                server = new NonBlockingServer(8080, numberOfThreads, statistics);
                break;
            case ASYNCHRONOUS_TYPE:
                server = new AsynchronousServer(8080, numberOfThreads, numberOfClients, statistics);
                break;
            default:
                throw new RuntimeException("Unexpected client type.");
        }
        server.start();
        /* just to make sure that server had enough time to start */
        Thread.sleep(2000);
        var clientRunner = new ClientRunner(numberOfClients, numberOfElements, timeBetweenMessagesMillis,
                numberOfMessages, "localhost", 8080, architecture, server, numberOfThreads, statistics);
        clientRunner.run();
    }
}
