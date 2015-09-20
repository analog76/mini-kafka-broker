package com.anand;

import kafka.utils.Time;

/**
 * Created by anand.ranganathan on 9/19/15.
 */

class  SystemTime implements Time {

    public long milliseconds() {
        return System.currentTimeMillis();
    }

    public long nanoseconds() {
        return System.nanoTime();
    }

    public void sleep(long ms) {
        try {
            Thread.sleep(ms);

        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }
}
