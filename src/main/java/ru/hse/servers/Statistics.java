package ru.hse.servers;

import com.google.common.util.concurrent.AtomicDouble;

import java.util.concurrent.atomic.AtomicLong;

public class Statistics {
    private final AtomicDouble totalServerTime = new AtomicDouble(0);
    private final AtomicLong totalServerCount = new AtomicLong(0);
    private final AtomicDouble totalClientTime = new AtomicDouble(0);
    private final AtomicLong totalClientCount = new AtomicLong(0);

    public void addServerTime(double time) {
        totalServerTime.addAndGet(time);
        totalServerCount.incrementAndGet();
    }

    public double getServerMean() {
        return totalServerTime.get() / totalServerCount.get();
    }

    public void addClientTime(double time) {
        totalClientTime.addAndGet(time);
        totalClientCount.incrementAndGet();
    }

    public double getClientMean() {
        return totalClientTime.get() / totalClientCount.get();
    }
}
