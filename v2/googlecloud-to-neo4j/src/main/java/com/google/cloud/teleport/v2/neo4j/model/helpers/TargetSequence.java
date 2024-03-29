package com.google.cloud.teleport.v2.neo4j.model.helpers;

import org.neo4j.importer.v1.targets.Target;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TargetSequence {

    private final Map<Target, Integer> targetSequences = new HashMap<>();
    private final AtomicInteger nextNumber = new AtomicInteger(0);

    public int getSequenceNumber(Target target) {
        return targetSequences.computeIfAbsent(target, (key) -> nextNumber.getAndIncrement());
    }
}
