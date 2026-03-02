package com.heaptest.core;

public class Address {
    private String street;
    private String city;
    private String state;
    private String zipCode;
    private String country;
    private double latitude;
    private double longitude;

    public Address(String street, String city, String state, String zipCode, String country, double latitude, double longitude) {
        this.street = street;
        this.city = city;
        this.state = state;
        this.zipCode = zipCode;
        this.country = country;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String getStreet() { return street; }
    public String getCity() { return city; }
    public String getState() { return state; }
    public String getZipCode() { return zipCode; }
    public String getCountry() { return country; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
}
