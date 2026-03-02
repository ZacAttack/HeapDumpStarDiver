package com.heaptest.hr;

import com.heaptest.core.BaseEntity;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class EmployeeDirectory extends BaseEntity {
    private Map<String, Employee> employeesById;
    private ConcurrentHashMap<String, List<Employee>> departmentIndex;
    private int totalCount;

    public EmployeeDirectory(long id) {
        super(id);
        this.employeesById = new HashMap<>();
        this.departmentIndex = new ConcurrentHashMap<>();
        this.totalCount = 0;
    }

    public void addEmployee(Employee e) {
        employeesById.put(e.getEmployeeId(), e);
        totalCount++;
    }

    public void indexByDepartment(String deptName, Employee e) {
        departmentIndex.computeIfAbsent(deptName, k -> new ArrayList<>()).add(e);
    }

    public Employee getByEmployeeId(String id) { return employeesById.get(id); }
    public Map<String, Employee> getEmployeesById() { return employeesById; }
    public ConcurrentHashMap<String, List<Employee>> getDepartmentIndex() { return departmentIndex; }
    public int getTotalCount() { return totalCount; }
}
