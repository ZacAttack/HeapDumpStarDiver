package com.heaptest.security;

import com.heaptest.core.BaseEntity;
import com.heaptest.core.CoreEnums;

public class ThreatEvent extends BaseEntity {
    private String source;
    private String target;
    private String type; // BRUTE_FORCE, SQL_INJECTION, XSS, DDOS, PHISHING
    private CoreEnums.Severity severity;
    private long detectedAt;
    private boolean mitigated;
    private String response;

    public ThreatEvent(long id, String source, String target, String type) {
        super(id);
        this.source = source;
        this.target = target;
        this.type = type;
        this.severity = CoreEnums.Severity.MAJOR;
        this.detectedAt = System.currentTimeMillis();
        this.mitigated = false;
    }

    public String getSource() { return source; }
    public String getTarget() { return target; }
    public String getType() { return type; }
    public CoreEnums.Severity getSeverity() { return severity; }
    public void setSeverity(CoreEnums.Severity s) { this.severity = s; }
    public long getDetectedAt() { return detectedAt; }
    public boolean isMitigated() { return mitigated; }
    public void setMitigated(boolean m) { this.mitigated = m; }
    public String getResponse() { return response; }
    public void setResponse(String response) { this.response = response; }
}
