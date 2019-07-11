package io.quantumdb.demo.utils;

import java.util.List;

import com.google.common.collect.Lists;


public class PerformanceTracker {

    public static enum Type {
        SELECT, UPDATE, INSERT, DELETE
    }

    private final List<Integer> inserts;
    private final List<Integer> selects;
    private final List<Integer> updates;
    private final List<Integer> deletes;

    public PerformanceTracker() {
        this.inserts = Lists.newCopyOnWriteArrayList();
        this.selects = Lists.newCopyOnWriteArrayList();
        this.updates = Lists.newCopyOnWriteArrayList();
        this.deletes = Lists.newCopyOnWriteArrayList();
    }

    public void reset() {
        inserts.clear();
        selects.clear();
        updates.clear();
        deletes.clear();
    }

    public void registerDuration(Type type, int duration) {
        switch (type) {
            case INSERT:
                inserts.add(duration);
                break;
            case SELECT:
                selects.add(duration);
                break;
            case UPDATE:
                updates.add(duration);
                break;
            case DELETE:
                deletes.add(duration);
                break;
            default:
                // Do nothing...
        }
    }

    public String generateOutput() {
        int index = 0;
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT\tSELECT\tUPDATE\tDELETE\n");

        while (true) {
            boolean printedData = false;
            if (inserts.size() > index) {
                builder.append(inserts.get(index));
                printedData = true;
            }
            builder.append("\t");
            if (selects.size() > index) {
                builder.append(selects.get(index));
                printedData = true;
            }
            builder.append("\t");
            if (updates.size() > index) {
                builder.append(updates.get(index));
                printedData = true;
            }
            builder.append("\t");
            if (deletes.size() > index) {
                builder.append(deletes.get(index));
                printedData = true;
            }
            builder.append("\n");

            if (!printedData) {
                break;
            }
            index++;
        }

        return builder.toString();
    }

    public String generateSimplifiedOutput() {
        StringBuilder builder = new StringBuilder();
        builder.append("DURATION\n");

        inserts.forEach(duration -> builder.append(duration + "\n"));
        selects.forEach(duration -> builder.append(duration + "\n"));
        updates.forEach(duration -> builder.append(duration + "\n"));
        deletes.forEach(duration -> builder.append(duration + "\n"));

        return builder.toString();
    }

}
