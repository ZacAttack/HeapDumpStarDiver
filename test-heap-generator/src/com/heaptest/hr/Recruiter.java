package com.heaptest.hr;

import java.util.ArrayList;
import java.util.List;

public class Recruiter extends Employee {
    private List<String> specializations;
    private int openPositions;
    private List<Employee> placements;
    private int totalPlacements;
    private double placementRate;

    public Recruiter(long id, String firstName, String lastName, String email, String employeeId) {
        super(id, firstName, lastName, email, employeeId);
        this.specializations = new ArrayList<>();
        this.placements = new ArrayList<>();
        this.openPositions = 0;
        this.totalPlacements = 0;
        this.placementRate = 0.0;
    }

    public List<String> getSpecializations() { return specializations; }
    public void addSpecialization(String spec) { specializations.add(spec); }
    public int getOpenPositions() { return openPositions; }
    public void setOpenPositions(int n) { this.openPositions = n; }
    public List<Employee> getPlacements() { return placements; }
    public void addPlacement(Employee e) { placements.add(e); totalPlacements++; }
    public int getTotalPlacements() { return totalPlacements; }
    public double getPlacementRate() { return placementRate; }
    public void setPlacementRate(double rate) { this.placementRate = rate; }
}
