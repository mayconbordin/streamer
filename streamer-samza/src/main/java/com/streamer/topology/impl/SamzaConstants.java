package com.streamer.topology.impl;

/**
 *
 * @author mayconbordin
 */
public interface SamzaConstants {
    String SYSTEM_NAME = "samza_system";
    String DEFAULT_SYSTEM_NAME = "kafka";
    
    interface Mode {
        String LOCAL = "local";
        String YARN  = "yarn";
    }
}
