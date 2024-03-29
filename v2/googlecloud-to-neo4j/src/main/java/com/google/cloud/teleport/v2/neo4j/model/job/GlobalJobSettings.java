package com.google.cloud.teleport.v2.neo4j.model.job;

import java.util.Collections;
import java.util.Map;

public class GlobalJobSettings {

    private final Map<String, Object> settings;

    private GlobalJobSettings(Map<String, Object> settings) {
        this.settings = settings;
    }

    public static GlobalJobSettings wrap(Map<String, Object> settings) {
        return new GlobalJobSettings(settings == null ? Collections.emptyMap() : settings);
    }


    public boolean resetDatabase() {
        return (boolean) settings.getOrDefault("reset_db", false);
    }
}
