package io.quantumdb.demo.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import com.google.common.collect.Lists;


public class PerformanceTracker {

    public static enum Type {
        SELECT, UPDATE, INSERT, DELETE
    }

    public static class ExecutionStats
    {
        private final Date startTime;
        private final Date endTime;
        private final Integer duration;
        private final int rowsAffected;
        private int threadNum;

        public ExecutionStats(Date startTime, Date endTime, int rowsAffected)
        {
            this.rowsAffected = rowsAffected;
            this.startTime = startTime;
            this.endTime = endTime;
            if (this.endTime != null) {
                this.duration = Math.toIntExact(this.endTime.getTime() - this.startTime.getTime());
            } else {
                this.duration = -1;
            }
        }

        public String toString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("(");
            stringBuilder.append("Thread: ");
            stringBuilder.append(this.threadNum);
            stringBuilder.append("  rows affected: ");
            stringBuilder.append(this.rowsAffected);
            stringBuilder.append("  ");
            stringBuilder.append(this.startTime.toString() + "\t");
            if (this.endTime != null) {
                stringBuilder.append(this.endTime.toString() + "\t");
            } else {
                stringBuilder.append("NULL \t\t\t");
            }
            stringBuilder.append(this.duration.toString());
            stringBuilder.append(")");
            return stringBuilder.toString();
        }

        public String toSimpleString()
        {
            return this.duration.toString();
        }

        public String toCSVEntry()
        {
            StringBuilder stringBuilder = new StringBuilder();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            stringBuilder.append(sdf.format(this.startTime));
            stringBuilder.append(",");
            stringBuilder.append(this.duration);
            return stringBuilder.toString();
        }

        public void setThreadNum(int threadNum) {
            this.threadNum = threadNum;
        }
    }

    private final List<ExecutionStats> inserts;
    private final List<ExecutionStats> selects;
    private final List<ExecutionStats> updates;
    private final List<ExecutionStats> deletes;

    private List<ExecutionStats> saved_inserts = new ArrayList<>();
    private List<ExecutionStats> saved_selects = new ArrayList<>();
    private List<ExecutionStats> saved_updates = new ArrayList<>();
    private List<ExecutionStats> saved_deletes = new ArrayList<>();

    public PerformanceTracker() {
        this.inserts = Lists.newCopyOnWriteArrayList();
        this.selects = Lists.newCopyOnWriteArrayList();
        this.updates = Lists.newCopyOnWriteArrayList();
        this.deletes = Lists.newCopyOnWriteArrayList();
    }

    public void reset() {
        saved_inserts.addAll(inserts);
        saved_selects.addAll(selects);
        saved_updates.addAll(updates);
        saved_deletes.addAll(deletes);

        inserts.clear();
        selects.clear();
        updates.clear();
        deletes.clear();
    }

    public void registerDuration(Type type, ExecutionStats time) {
        switch (type) {
            case INSERT:
                inserts.add(time);
                break;
            case SELECT:
                selects.add(time);
                break;
            case UPDATE:
                updates.add(time);
                break;
            case DELETE:
                deletes.add(time);
                break;
            default:
                // Do nothing...
        }
    }

    public String generateOutput() {
        int index = 0;
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT\tSELECT\tUPDATE\tDELETE");
        builder.append(System.lineSeparator());

        while (true) {
            boolean printedData = false;
            if (inserts.size() > index) {
                builder.append(inserts.get(index).toString());
                printedData = true;
            }
            builder.append("\t");
            if (selects.size() > index) {
                builder.append(selects.get(index).toString());
                printedData = true;
            }
            builder.append("\t");
            if (updates.size() > index) {
                builder.append(updates.get(index).toString());
                printedData = true;
            }
            builder.append("\t");
            if (deletes.size() > index) {
                builder.append(deletes.get(index).toString());
                printedData = true;
            }
            builder.append(System.lineSeparator());

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

        builder.append("Inserts\n");
        inserts.forEach(duration -> builder.append(duration.toSimpleString() + "\n"));
        builder.append("Selects\n");
        selects.forEach(duration -> builder.append(duration .toSimpleString()+ "\n"));
        builder.append("Updates\n");
        updates.forEach(duration -> builder.append(duration.toSimpleString() + "\n"));
        builder.append("Deletes\n");
        deletes.forEach(duration -> builder.append(duration.toSimpleString() + "\n"));

        return builder.toString();
    }

    public void saveOutputToCSV(String filePrefix)
    {
        saveOutputToCSV(filePrefix + "_inserts.csv", this.saved_inserts);
        saveOutputToCSV(filePrefix + "_selects.csv", this.saved_selects);
        saveOutputToCSV(filePrefix + "_updates.csv", this.saved_updates);
        saveOutputToCSV(filePrefix + "_deletes.csv", this.saved_deletes);
        resetSavedExecutionTimes();
    }

    public void savePerformanceStatistics(String ddl, ExecutionStats ddlExecutionStats) {
        if (ddlExecutionStats == null || ddlExecutionStats.startTime == null || ddlExecutionStats.endTime == null) {
            System.out.println("Unable to dump statistics for " + ddl + ". Can not obtain the start and end times for the operation");
            resetSavedExecutionTimes();
            return;
        }
        savePerformanceStatistics(ddl, ddlExecutionStats, "inserts", saved_inserts);
        savePerformanceStatistics(ddl, ddlExecutionStats, "selects", saved_selects);
        savePerformanceStatistics(ddl, ddlExecutionStats, "updates", saved_updates);
        savePerformanceStatistics(ddl, ddlExecutionStats, "deletes", saved_deletes);
        resetSavedExecutionTimes();
    }

    private void resetSavedExecutionTimes() {
        saved_inserts.clear();
        saved_selects.clear();
        saved_updates.clear();
        saved_deletes.clear();
    }

    private static void savePerformanceStatistics(String ddl, ExecutionStats ddlExecutionStats, String dmlName, List<ExecutionStats> saved_dml) {
        if (saved_dml.isEmpty()) {
            System.out.println("No " + dmlName + " has been executed during DDL " + ddl);
            return;
        }
        try {
            FileWriter statisticsFile = new FileWriter("stats.txt", true);
            FileWriter dmlStatisticsFile = new FileWriter(ddl + "_" + dmlName + ".txt");
            Integer beforeDDLMean = 0;
            Integer duringDDLMean = 0;
            Integer afterDDLMean = 0;
            int beforeDDLCount = 0;
            int duringDDLCount = 0;
            int afterDDLCount = 0;
            boolean isBeforeSection = false;
            boolean isDuringSection = false;
            boolean isAfterSection = false;
            int skippedExecutionTimes = 0;
            int rowsAffectedBeforeDDL = 0;
            int rowsAffectedDuringDDL = 0;
            int rowsAffectedAfterDDL = 0;
            HashSet threadsBefore = new HashSet();
            HashSet threadsDuring = new HashSet();
            HashSet threadsAfter = new HashSet();
            for (ExecutionStats exTime : saved_dml) {
                if (exTime == null || exTime.startTime == null || exTime.endTime == null) {
                    ++skippedExecutionTimes;
                    continue;
                }
                if (exTime.startTime.before(ddlExecutionStats.startTime) && (exTime.endTime.before(ddlExecutionStats.startTime) || exTime.endTime.compareTo(ddlExecutionStats.startTime) == 0)) {
                    ++beforeDDLCount;
                    isBeforeSection = (beforeDDLCount == 1);
                    beforeDDLMean += exTime.duration;
                    rowsAffectedBeforeDDL += exTime.rowsAffected;
                    threadsBefore.add(exTime.threadNum);
                } else if ((exTime.startTime.before(ddlExecutionStats.startTime) || exTime.startTime.compareTo(ddlExecutionStats.startTime) == 0)
                        && exTime.endTime.after(ddlExecutionStats.startTime)) {
                    ++duringDDLCount;
                    isDuringSection = (duringDDLCount == 1);
                    duringDDLMean += exTime.duration;
                    rowsAffectedDuringDDL += exTime.rowsAffected;
                    threadsDuring.add(exTime.threadNum);
                } else if (exTime.startTime.after(ddlExecutionStats.startTime)
                        && (exTime.endTime.before(ddlExecutionStats.endTime) || exTime.endTime.compareTo(ddlExecutionStats.endTime) == 0)) {
                    ++duringDDLCount;
                    isDuringSection = (duringDDLCount == 1);
                    duringDDLMean += exTime.duration;
                    rowsAffectedDuringDDL += exTime.rowsAffected;
                    threadsDuring.add(exTime.threadNum);
                } else if ((exTime.startTime.after(ddlExecutionStats.startTime) || exTime.startTime.compareTo(ddlExecutionStats.startTime) == 0)
                        && exTime.startTime.before(ddlExecutionStats.endTime)) {
                    ++duringDDLCount;
                    isDuringSection = (duringDDLCount == 1);
                    duringDDLMean += exTime.duration;
                    rowsAffectedDuringDDL += exTime.rowsAffected;
                    threadsDuring.add(exTime.threadNum);
                }  else if ((exTime.startTime.compareTo(exTime.endTime) == 0 && exTime.startTime.compareTo(ddlExecutionStats.startTime) == 0)
                        || (exTime.startTime.compareTo(exTime.endTime) == 0 && exTime.startTime.compareTo(ddlExecutionStats.endTime) == 0)) {
                    ++duringDDLCount;
                    isDuringSection = (duringDDLCount == 1);
                    duringDDLMean += exTime.duration;
                    rowsAffectedDuringDDL += exTime.rowsAffected;
                    threadsDuring.add(exTime.threadNum);
                } else if ((exTime.startTime.after(ddlExecutionStats.endTime) || exTime.startTime.compareTo(ddlExecutionStats.endTime) == 0)
                        && exTime.endTime.after(ddlExecutionStats.endTime)) {
                    ++afterDDLCount;
                    isAfterSection = (afterDDLCount == 1);
                    afterDDLMean += exTime.duration;
                    rowsAffectedAfterDDL += exTime.rowsAffected;
                    threadsAfter.add(exTime.threadNum);
                } else {
                    System.out.println("DDL execution time is " + ddlExecutionStats.toString());
                    System.out.println("DML is in invalid range" + dmlName + " " + exTime.toString());
                }

                if (isBeforeSection) {
                    dmlStatisticsFile.append(dmlName + ": performance before " + ddl);
                    dmlStatisticsFile.append(System.lineSeparator());
                } else if (isDuringSection) {
                    dmlStatisticsFile.append(dmlName + ": performance during " + ddl);
                    dmlStatisticsFile.append(System.lineSeparator());
                } else if (isAfterSection) {
                    dmlStatisticsFile.append(dmlName + ": performance after " + ddl);
                    dmlStatisticsFile.append(System.lineSeparator());
                }
                dmlStatisticsFile.append(exTime.duration.toString() + ", ");
            }
            dumpStatisticsToFile(dmlStatisticsFile, dmlName, ddl, ddlExecutionStats, beforeDDLMean/(double)beforeDDLCount, duringDDLMean/(double)duringDDLCount, afterDDLMean/(double)afterDDLCount,
                    beforeDDLCount, duringDDLCount, afterDDLCount, rowsAffectedBeforeDDL, rowsAffectedDuringDDL, rowsAffectedAfterDDL, threadsBefore.size(), threadsDuring.size(), threadsAfter.size(), skippedExecutionTimes);
            dmlStatisticsFile.close();
            dumpStatisticsToFile(statisticsFile, dmlName, ddl, ddlExecutionStats, beforeDDLMean/(double)beforeDDLCount, duringDDLMean/(double)duringDDLCount, afterDDLMean/(double)afterDDLCount,
                    beforeDDLCount, duringDDLCount, afterDDLCount,  rowsAffectedBeforeDDL, rowsAffectedDuringDDL, rowsAffectedAfterDDL, threadsBefore.size(), threadsDuring.size(), threadsAfter.size(), skippedExecutionTimes);
            statisticsFile.close();
            if (duringDDLMean.intValue() == 0) {
                System.out.println("Repeat run for ddl " + ddl + " and dml " + dmlName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void dumpStatisticsToFile(FileWriter statsFile, String dmlName, String ddl, ExecutionStats ddlExecutionStats, double beforeDdl, double duringDdl, double afterDdl,
                                             int beforeDDLCount, int duringDDLCount, int afterDDLCount, int rowsAffectedBefore, int rowsAffectedDuring, int rowsAffectedAfter,
                                             int threadsBefore, int threadsDuring, int threadsAfter, int skippedExecutionTimes) throws IOException {
        statsFile.append(System.lineSeparator());
        statsFile.append(ddl + " execution time: " + ddlExecutionStats.startTime.toString() + "- " + ddlExecutionStats.endTime.toString() + ": " + ddlExecutionStats.duration + " milliseconds.");
        statsFile.append(System.lineSeparator());
        statsFile.append("Mean execution time before " + dmlName + ": " + beforeDdl + ". Number of dmls: " + beforeDDLCount);
        statsFile.append(System.lineSeparator());
        statsFile.append("Rows affected before ddl: " + rowsAffectedBefore);
        statsFile.append(System.lineSeparator());
        statsFile.append("Number for different threads running before ddl: " + threadsBefore);
        statsFile.append(System.lineSeparator());
        statsFile.append("Mean execution time during " + dmlName + ": " + duringDdl + ". Number of dmls: " + duringDDLCount);
        statsFile.append(System.lineSeparator());
        statsFile.append("Rows affected during ddl: " + rowsAffectedDuring);
        statsFile.append(System.lineSeparator());
        statsFile.append("Number for different threads running during ddl: " + threadsDuring);
        statsFile.append(System.lineSeparator());
        statsFile.append("Mean execution time after " + dmlName + ": " + afterDdl + ". Number of dmls: " + afterDDLCount);
        statsFile.append(System.lineSeparator());
        statsFile.append("Rows affected after ddl: " + rowsAffectedAfter);
        statsFile.append(System.lineSeparator());
        statsFile.append("Number for different threads running after ddl: " + threadsAfter);
        statsFile.append(System.lineSeparator());
        statsFile.append("Skipped " + skippedExecutionTimes + " dml malformed execution times.");
        statsFile.append(System.lineSeparator());
        statsFile.append("***********************************************************************");
        statsFile.append(System.lineSeparator());
        statsFile.append(System.lineSeparator());
        statsFile.flush();
    }

    private void saveOutputToCSV(String fileName, List<ExecutionStats> saved_entries) {
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(fileName);
            for (ExecutionStats executionStats : saved_entries) {
                fileWriter.append(executionStats.toCSVEntry());
                fileWriter.append(System.lineSeparator());
            }
            if (fileWriter != null) {
                fileWriter.flush();
                fileWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
