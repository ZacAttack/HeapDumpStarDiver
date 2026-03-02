package com.heaptest.comms;

import com.heaptest.core.BaseEntity;

public class Attachment extends BaseEntity {
    private String filename;
    private String mimeType;
    private long sizeBytes;
    private byte[] data;
    private String checksum;

    public Attachment(long id, String filename, String mimeType, byte[] data) {
        super(id);
        this.filename = filename;
        this.mimeType = mimeType;
        this.data = data;
        this.sizeBytes = data != null ? data.length : 0;
    }

    public String getFilename() { return filename; }
    public String getMimeType() { return mimeType; }
    public long getSizeBytes() { return sizeBytes; }
    public byte[] getData() { return data; }
    public String getChecksum() { return checksum; }
    public void setChecksum(String checksum) { this.checksum = checksum; }
}
