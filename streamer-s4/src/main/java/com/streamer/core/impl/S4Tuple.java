package com.streamer.core.impl;

import com.streamer.core.Tuple;
import org.apache.s4.base.Event;

/**
 *
 * @author mayconbordin
 */
public class S4Tuple extends Event {
    private String key;
    private Tuple value;

    public S4Tuple() {
    }
    
    public S4Tuple(Tuple value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Tuple getValue() {
        return value;
    }

    public void setValue(Tuple value) {
        this.value = value;
    }
}
