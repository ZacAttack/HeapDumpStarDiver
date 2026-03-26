// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.hr;

import com.heaptest.core.TaggableEntity;

public abstract class Person extends TaggableEntity {
    protected String firstName;
    protected String lastName;
    protected String email;
    protected byte[] photoThumbnail;
    protected long dateOfBirth;
    protected String phoneNumber;

    public Person(long id, String firstName, String lastName, String email) {
        super(id, "system");
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.dateOfBirth = 0L;
    }

    public String getFirstName() { return firstName; }
    public String getLastName() { return lastName; }
    public String getEmail() { return email; }
    public byte[] getPhotoThumbnail() { return photoThumbnail; }
    public void setPhotoThumbnail(byte[] photo) { this.photoThumbnail = photo; }
    public long getDateOfBirth() { return dateOfBirth; }
    public void setDateOfBirth(long dob) { this.dateOfBirth = dob; }
    public String getPhoneNumber() { return phoneNumber; }
    public void setPhoneNumber(String phone) { this.phoneNumber = phone; }
}
