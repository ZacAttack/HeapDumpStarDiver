package com.heaptest.infra;

import com.heaptest.core.AuditableEntity;

public class SslCertificate extends AuditableEntity {
    private String domain;
    private byte[] fingerprint;
    private long expiryDate;
    private String issuer;
    private String algorithm;
    private boolean wildcard;
    private int keySize;

    public SslCertificate(long id, String domain, byte[] fingerprint) {
        super(id, "system");
        this.domain = domain;
        this.fingerprint = fingerprint;
        this.expiryDate = System.currentTimeMillis() + 365L * 24 * 60 * 60 * 1000;
        this.keySize = 2048;
    }

    public String getDomain() { return domain; }
    public byte[] getFingerprint() { return fingerprint; }
    public long getExpiryDate() { return expiryDate; }
    public void setExpiryDate(long date) { this.expiryDate = date; }
    public String getIssuer() { return issuer; }
    public void setIssuer(String issuer) { this.issuer = issuer; }
    public String getAlgorithm() { return algorithm; }
    public void setAlgorithm(String alg) { this.algorithm = alg; }
    public boolean isWildcard() { return wildcard; }
    public void setWildcard(boolean w) { this.wildcard = w; }
    public int getKeySize() { return keySize; }
}
