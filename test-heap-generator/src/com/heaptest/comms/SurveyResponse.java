// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

package com.heaptest.comms;

import com.heaptest.core.BaseEntity;
import java.util.HashMap;
import java.util.Map;

public class SurveyResponse extends BaseEntity {
    private Object respondent; // hr.Employee
    private Map<String, String> answers;
    private long submittedAt;
    private String surveyName;
    private boolean anonymous;

    public SurveyResponse(long id, Object respondent, String surveyName) {
        super(id);
        this.respondent = respondent;
        this.surveyName = surveyName;
        this.answers = new HashMap<>();
        this.submittedAt = System.currentTimeMillis();
        this.anonymous = false;
    }

    public Object getRespondent() { return respondent; }
    public Map<String, String> getAnswers() { return answers; }
    public void addAnswer(String question, String answer) { answers.put(question, answer); }
    public long getSubmittedAt() { return submittedAt; }
    public String getSurveyName() { return surveyName; }
    public boolean isAnonymous() { return anonymous; }
    public void setAnonymous(boolean anon) { this.anonymous = anon; }
}
