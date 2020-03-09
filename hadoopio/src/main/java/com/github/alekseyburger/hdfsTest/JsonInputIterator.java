package com.github.alekseyburger.hdfsTest;

import java.util.Iterator;

public class JsonInputIterator <T> implements Iterator<String> {
    private Integer count = 0;
    private final Integer max = 100000000;


    public boolean hasNext() {
        return (count<max);
    }

    public String  next() {
        count++;
        return "{ \"text\" = \"Line\", \"count\" = " + count.toString() + " }\n";
    }

    public void remove() {
    }
}
