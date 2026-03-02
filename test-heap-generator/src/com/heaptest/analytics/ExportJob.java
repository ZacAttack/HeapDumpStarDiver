package com.heaptest.analytics;

import com.heaptest.core.AuditableEntity;
import com.heaptest.core.CoreEnums;

public class ExportJob extends AuditableEntity {
    private Report report;
    private String format; // CSV, PDF, XLSX
    private CoreEnums.Status status;
    private String outputPath;
    private Object requestedBy; // hr.Employee
    private long completedAt;
    private long fileSizeBytes;

    public ExportJob(long id, Report report, String format) {
        super(id, "system");
        this.report = report;
        this.format = format;
        this.status = CoreEnums.Status.PENDING;
    }

    public Report getReport() { return report; }
    public String getFormat() { return format; }
    public CoreEnums.Status getStatus() { return status; }
    public void setStatus(CoreEnums.Status s) { this.status = s; }
    public String getOutputPath() { return outputPath; }
    public void setOutputPath(String path) { this.outputPath = path; }
    public Object getRequestedBy() { return requestedBy; }
    public void setRequestedBy(Object user) { this.requestedBy = user; }
    public long getCompletedAt() { return completedAt; }
    public void setCompletedAt(long ts) { this.completedAt = ts; }
    public long getFileSizeBytes() { return fileSizeBytes; }
    public void setFileSizeBytes(long size) { this.fileSizeBytes = size; }
}
