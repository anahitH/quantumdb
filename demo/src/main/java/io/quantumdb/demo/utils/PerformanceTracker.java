package io.quantumdb.demo.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class PerformanceTracker {

    public static enum Type {
        SELECT, UPDATE, INSERT, DELETE
    }

    private List<ExecutionStats> inserts = new ArrayList<>();
    private List<ExecutionStats> selects = new ArrayList<>();
    private List<ExecutionStats> updates = new ArrayList<>();
    private List<ExecutionStats> deletes = new ArrayList<>();

    public void reset() {
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

    public void savePerformanceStatistics(String ddl, List<ExecutionStats> ddlExecutionStats) {
        if (ddlExecutionStats.isEmpty()) {
            System.out.println("Unable to dump statistics for " + ddl + ". Can not obtain the start and end times for the operation");
            resetSavedExecutionTimes();
            return;
        }
        savePerformanceStatistics(ddl, ddlExecutionStats, "inserts", inserts);
        savePerformanceStatistics(ddl, ddlExecutionStats, "selects", selects);
        savePerformanceStatistics(ddl, ddlExecutionStats, "updates", updates);
        savePerformanceStatistics(ddl, ddlExecutionStats, "deletes", deletes);
        resetSavedExecutionTimes();
    }

    private void resetSavedExecutionTimes() {
        inserts.clear();
        selects.clear();
        updates.clear();
        deletes.clear();
    }

    private static void savePerformanceStatistics(String ddl, List<ExecutionStats> ddlExecutionStats, String dmlName, List<ExecutionStats> dmlExecutionStats) {
        if (dmlExecutionStats.isEmpty()) {
            return;
        }
        JSONObject jsonDoc = new JSONObject();
        JSONObject ddlObj = new JSONObject();

        JSONArray ddlExecutionTimes = new JSONArray();
        ddlExecutionStats.forEach(exTime -> { if (exTime != null && exTime.isValidExecutionStat()) {
            ddlExecutionTimes.add(exTime.duration);
        }  });
        ddlObj.put("ddls", ddlExecutionTimes);

        JSONArray dmlsBeforeDlls = new JSONArray();
        dmlExecutionStats.forEach(dmlExTime -> {
            if (dmlExTime.isValidExecutionStat() && isDmlBeforeDdls(dmlExTime, ddlExecutionStats)) {
                dmlsBeforeDlls.add(dmlExTime.duration);

            }});
        ddlObj.put("DML_before_DDL", dmlsBeforeDlls);

        JSONArray dmlsDuringDlls = new JSONArray();
        HashSet<Integer> uniqueThreads = new HashSet();
        dmlExecutionStats.forEach(dmlExTime -> {
            if (dmlExTime.isValidExecutionStat() && isDmlDuringDdls(dmlExTime, ddlExecutionStats)) {
                dmlsDuringDlls.add(dmlExTime.duration);
                uniqueThreads.add(dmlExTime.threadNum);

            }});
        ddlObj.put("DML_during_DDL", dmlsDuringDlls);
        ddlObj.put("Num_of_threads_during_ddl", uniqueThreads.size());

        JSONArray dmlsAfterDlls = new JSONArray();
        dmlExecutionStats.forEach(dmlExTime -> {
            if (dmlExTime.isValidExecutionStat() && isDmlAfterDdls(dmlExTime, ddlExecutionStats)) {
                dmlsAfterDlls.add(dmlExTime.duration);

            }});
        ddlObj.put("DML_after_DDL", dmlsAfterDlls);

        JSONObject dmlObj = new JSONObject();
        dmlObj.put(dmlName, ddlObj);
        jsonDoc.put(ddl, dmlObj);

        try {
            FileWriter fileWriter = new FileWriter(ddl + "_" + dmlName + ".json");
            fileWriter.write(jsonDoc.toJSONString());
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("DML ran during schema modification " + dmlsDuringDlls.size());
    }

    private static boolean isDmlBeforeDdls(ExecutionStats dmlExTime, List<ExecutionStats> ddlExecutionStats) {
        for (ExecutionStats ddlStats : ddlExecutionStats) {
            if (isDmlBeforeDDl(dmlExTime, ddlStats))  {
                return true;
            }
        }
        return false;
    }

    private static boolean isDmlDuringDdls(ExecutionStats dmlExTime, List<ExecutionStats> ddlExecutionStats) {
        for (ExecutionStats ddlStats : ddlExecutionStats) {
            if (isDmlDuringDDl(dmlExTime, ddlStats))  {
                return true;
            }
        }
        return false;
    }

    private static boolean isDmlAfterDdls(ExecutionStats dmlExTime, List<ExecutionStats> ddlExecutionStats) {
        for (ExecutionStats ddlStats : ddlExecutionStats) {
            if (isDmlAfterDDl(dmlExTime, ddlStats))  {
                return true;
            }
        }
        return false;
    }

    private static boolean isDmlBeforeDDl(ExecutionStats dmlExTime, ExecutionStats ddlStats) {
        if (!dmlExTime.isValidExecutionStat() || !ddlStats.isValidExecutionStat()) {
            return false;
        }
        if (dmlExTime.startTime.before(ddlStats.startTime)
                && (dmlExTime.endTime.before(ddlStats.startTime) || dmlExTime.endTime.compareTo(ddlStats.startTime) == 0)) {
            return true;
        }
        return false;
    }

    private static boolean isDmlDuringDDl(ExecutionStats dmlExTime, ExecutionStats ddlStats) {
        if (!dmlExTime.isValidExecutionStat() || !ddlStats.isValidExecutionStat()) {
            return false;
        }
        if (((dmlExTime.startTime.before(ddlStats.startTime)
                || dmlExTime.startTime.compareTo(ddlStats.startTime) == 0)  && dmlExTime.endTime.after(ddlStats.startTime))
        || (dmlExTime.startTime.after(ddlStats.startTime)
                && (dmlExTime.endTime.before(ddlStats.endTime) || dmlExTime.endTime.compareTo(ddlStats.endTime) == 0))
        || ((dmlExTime.startTime.after(ddlStats.startTime) || dmlExTime.startTime.compareTo(ddlStats.startTime) == 0)
                && dmlExTime.startTime.before(ddlStats.endTime))
        || ((dmlExTime.startTime.compareTo(dmlExTime.endTime) == 0 && dmlExTime.startTime.compareTo(ddlStats.startTime) == 0)
                || (dmlExTime.startTime.compareTo(dmlExTime.endTime) == 0 && dmlExTime.startTime.compareTo(ddlStats.endTime) == 0))) {
            return true;
        }
        return false;
    }

    private static boolean isDmlAfterDDl(ExecutionStats dmlExTime, ExecutionStats ddlStats) {
        if (!dmlExTime.isValidExecutionStat() || !ddlStats.isValidExecutionStat()) {
            return false;
        }
        if ((dmlExTime.startTime.after(ddlStats.endTime) || dmlExTime.startTime.compareTo(ddlStats.endTime) == 0)
                && dmlExTime.endTime.after(ddlStats.endTime))  {
            return true;
        }
        return false;
    }

    /*
    private static void savePerformanceStatistics(String ddl, List<ExecutionStats> ddlExecutionStats, String dmlName, List<ExecutionStats> saved_dml) {
        // At this point dump json file with all necessary info
        // later process the json file with python script and dump more specific statistics
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
*/
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
