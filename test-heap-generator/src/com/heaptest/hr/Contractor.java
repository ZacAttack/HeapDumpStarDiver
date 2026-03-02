package com.heaptest.hr;

public class Contractor extends Person {
    private String company;
    private long contractEnd;
    private double hourlyRate;
    private int maxHoursPerWeek;
    private String projectName;
    private boolean renewable;

    public Contractor(long id, String firstName, String lastName, String email, String company) {
        super(id, firstName, lastName, email);
        this.company = company;
        this.contractEnd = System.currentTimeMillis() + 180L * 24 * 60 * 60 * 1000;
        this.hourlyRate = 0.0;
        this.maxHoursPerWeek = 40;
        this.renewable = true;
    }

    public String getCompany() { return company; }
    public long getContractEnd() { return contractEnd; }
    public void setContractEnd(long date) { this.contractEnd = date; }
    public double getHourlyRate() { return hourlyRate; }
    public void setHourlyRate(double rate) { this.hourlyRate = rate; }
    public int getMaxHoursPerWeek() { return maxHoursPerWeek; }
    public void setMaxHoursPerWeek(int max) { this.maxHoursPerWeek = max; }
    public String getProjectName() { return projectName; }
    public void setProjectName(String name) { this.projectName = name; }
    public boolean isRenewable() { return renewable; }
}
