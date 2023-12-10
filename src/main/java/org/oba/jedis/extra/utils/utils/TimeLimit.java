package org.oba.jedis.extra.utils.utils;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Class to control time elapsed from moment of creation of this object
 */
public class TimeLimit {



    private final long timeLimitMs;
    private boolean inLimit;

    /**
     * Creates a TimeLimit from now to this time quantity, in milliseconds
     * @param timeInMs amount of time in millisecods
     */
    public TimeLimit(long timeInMs) {
        this(timeInMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a TimeLimit from now to this time quantity
     * @param time amount of time
     * @param unit unit of time
     */
    public TimeLimit(long time, TimeUnit unit) {
        if (time <= 0) throw new IllegalArgumentException("Time must be more than zero");
        this.timeLimitMs = System.currentTimeMillis() + unit.toMillis(time);
        checkInLimit();
    }

    /**
     * Checks now if time is expired
     * @return true if time NOT expired
     */
    public synchronized boolean checkInLimit() {
        return inLimit = timeLimitMs > System.currentTimeMillis();
    }

    /**
     * Return the time resting to expiring, zero if expired
     * In milliseconds
     * Also, checks now if time is expired
     * @return time to expire
     */
    public synchronized long checkTimeRest() {
        if (checkInLimit()) {
            return timeLimitMs - System.currentTimeMillis();
        } else {
            return 0L;
        }
    }

    /**
     * Returns the last time check result
     * THIS METHOD DOES NOT CHECK THE CURRENT TIME FOR EXPIRATION
     * @return true if time NOT expired wihen checked
     */
    public boolean isInLimit() {
        return inLimit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeLimit timeLimit = (TimeLimit) o;
        return timeLimitMs == timeLimit.timeLimitMs && inLimit == timeLimit.inLimit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeLimitMs, inLimit);
    }

    @Override
    public String toString() {
        return "TimeLimit{" +
                "timeLimitMs=" + timeLimitMs +
                ", inLimit=" + inLimit +
                '}';
    }

}
