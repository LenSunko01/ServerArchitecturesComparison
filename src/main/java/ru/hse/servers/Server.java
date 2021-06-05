package ru.hse.servers;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Server {
    private final Statistics statistics;
    private final AtomicBoolean calculating = new AtomicBoolean(true);

    public Server(Statistics statistics) {
        this.statistics = statistics;
    }

    protected ArrayList<Integer> bubbleSort(ArrayList<Integer> list) {
        var startTime = System.nanoTime();
        for (int i = 0; i < list.size() - 1; i++) {
            for (int j = 0; j < list.size() - i - 1; j++) {
                if (list.get(j) > list.get(j + 1)) {
                    var tmp = list.get(j);
                    list.set(j, list.get(j + 1));
                    list.set(j + 1, tmp);
                }
            }
        }
        var finishTime = System.nanoTime();
        if (calculating.get()) {
            statistics.addServerTime((finishTime - startTime) / 1000000.0);
        }
        return list;
    }

    public abstract void start();
    public abstract void stop();

    public void finishStatistics() {
        calculating.set(false);
    }
}
