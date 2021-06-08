package ru.hse.servers;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public abstract class Client {
    protected final int numberOfElements;
    protected final int timeBetweenMessagesInMilliSeconds;
    protected final int numberOfMessages;
    protected final String serverHost;
    protected final int serverPort;
    protected final ClientRunner clientRunner;

    protected static ArrayList<Integer> generateArray(int numberOfElements) {
        var list = new ArrayList<Integer>(numberOfElements);
        Random rd = new Random();
        for (var i = 0; i < numberOfElements; i++) {
            list.add(rd.nextInt());
        }
        return list;
    }

    protected static boolean checkIsSorted(List<Integer> expected, List<Integer> actualList) {
        return expected.stream()
                .sorted()
                .collect(Collectors.toList())
                .equals(actualList);
    }

    public Client(
            int numberOfElements,
            int timeBetweenMessagesInMilliSeconds,
            int numberOfMessages,
            String serverHost,
            int serverPort,
            ClientRunner clientRunner) {
        this.numberOfElements = numberOfElements;
        this.timeBetweenMessagesInMilliSeconds = timeBetweenMessagesInMilliSeconds;
        this.numberOfMessages = numberOfMessages;
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.clientRunner = clientRunner;
    }

    public abstract void run();
}
