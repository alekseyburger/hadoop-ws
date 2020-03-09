package com.github.alekseyburger.hdfsTest;

import java.util.Iterator;

public class InputCollection<E> implements Iterable<String>{

    public Iterator<String> iterator() {
        return new JsonInputIterator<E>();
    }
}
