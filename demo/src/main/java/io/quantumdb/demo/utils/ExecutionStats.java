package io.quantumdb.demo.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ExecutionStats
{
    public final Date startTime;
    public final Date endTime;
    public final Integer duration;
    public final int rowsAffected;
    public int threadNum;

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

    public boolean isValidExecutionStat() {
        return startTime != null && endTime != null;
    }
}

