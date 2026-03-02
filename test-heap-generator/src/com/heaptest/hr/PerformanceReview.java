package com.heaptest.hr;

import com.heaptest.core.AuditableEntity;

public class PerformanceReview extends AuditableEntity {
    private Employee employee;
    private Employee reviewer;
    private float rating;
    private float[] categoryScores;
    private String comments;
    private String period;
    private boolean finalized;

    public PerformanceReview(long id, Employee employee, Employee reviewer) {
        super(id, "system");
        this.employee = employee;
        this.reviewer = reviewer;
        this.rating = 0.0f;
        this.categoryScores = new float[5]; // communication, technical, leadership, teamwork, initiative
        this.finalized = false;
    }

    public Employee getEmployee() { return employee; }
    public Employee getReviewer() { return reviewer; }
    public float getRating() { return rating; }
    public void setRating(float rating) { this.rating = rating; }
    public float[] getCategoryScores() { return categoryScores; }
    public void setCategoryScore(int idx, float score) { categoryScores[idx] = score; }
    public String getComments() { return comments; }
    public void setComments(String comments) { this.comments = comments; }
    public String getPeriod() { return period; }
    public void setPeriod(String period) { this.period = period; }
    public boolean isFinalized() { return finalized; }
    public void setFinalized(boolean f) { this.finalized = f; }
}
