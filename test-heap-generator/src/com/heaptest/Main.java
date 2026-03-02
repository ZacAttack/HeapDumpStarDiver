package com.heaptest;

import com.heaptest.core.*;
import com.heaptest.org.*;
import com.heaptest.hr.*;
import com.heaptest.finance.*;
import com.heaptest.project.*;
import com.heaptest.infra.*;
import com.heaptest.inventory.*;
import com.heaptest.comms.*;
import com.heaptest.analytics.*;
import com.heaptest.security.*;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.*;
import com.heaptest.finance.Currency;

public class Main {

    static double SCALE = 1.0;

    static int s(int base) { return Math.max(1, (int)(base * SCALE)); }

    static final Random RNG = new Random(42);
    static final IdGenerator ID_GEN = new IdGenerator(0);

    static final String[] FIRST_NAMES = {"Alice","Bob","Carol","Dave","Eve","Frank","Grace","Hank","Ivy","Jack",
        "Kate","Leo","Mia","Nick","Olive","Pat","Quinn","Ray","Sara","Tom","Uma","Vic","Wendy","Xena","Yuri","Zara"};
    static final String[] LAST_NAMES = {"Smith","Jones","Brown","Davis","Wilson","Taylor","Clark","Hall","Lee","Young",
        "King","Wright","Green","Adams","Baker","Nelson","Hill","Moore","White","Harris"};
    static final String[] CITIES = {"New York","London","Tokyo","Paris","Berlin","Sydney","Toronto","Mumbai","Sao Paulo","Lagos"};
    static final String[] STATES = {"NY","CA","TX","WA","IL","FL","OR","CO","MA","VA"};
    static final String[] COUNTRIES = {"US","UK","JP","FR","DE","AU","CA","IN","BR","NG"};
    static final String[] STREETS = {"123 Main St","456 Oak Ave","789 Pine Rd","321 Elm Blvd","654 Maple Dr",
        "987 Cedar Ln","147 Birch Way","258 Spruce Ct","369 Willow Pl","741 Ash Ter"};

    // ---- storage fields to keep everything reachable ----
    static List<Configuration> configurations = new ArrayList<>();
    static List<Address> addresses = new ArrayList<>();
    static List<DateRange> dateRanges = new ArrayList<>();

    static List<Organization> organizations = new ArrayList<>();
    static List<Department> departments = new ArrayList<>();
    static List<Team> teams = new ArrayList<>();
    static List<Division> divisions = new ArrayList<>();
    static List<OrgChart> orgCharts = new ArrayList<>();
    static List<Office> offices = new ArrayList<>();
    static List<OrgPolicy> orgPolicies = new ArrayList<>();
    static List<CostCenter> costCenters = new ArrayList<>();
    static List<OrgEvent> orgEvents = new ArrayList<>();
    static List<Subsidiary> subsidiaries = new ArrayList<>();
    static List<Committee> committees = new ArrayList<>();
    static List<OrgMetrics> orgMetricsList = new ArrayList<>();

    static List<Employee> employees = new ArrayList<>();
    static List<Contractor> contractors = new ArrayList<>();
    static List<Recruiter> recruiters = new ArrayList<>();
    static List<Role> roles = new ArrayList<>();
    static List<Payroll> payrolls = new ArrayList<>();
    static List<Benefits> benefitsList = new ArrayList<>();
    static List<PerformanceReview> reviews = new ArrayList<>();
    static List<Attendance> attendances = new ArrayList<>();
    static List<TrainingRecord> trainingRecords = new ArrayList<>();
    static List<LeaveRequest> leaveRequests = new ArrayList<>();
    static List<EmployeeDirectory> directories = new ArrayList<>();

    static List<Account> accounts = new ArrayList<>();
    static List<Transaction> transactions = new ArrayList<>();
    static List<Budget> budgets = new ArrayList<>();
    static List<Invoice> invoices = new ArrayList<>();
    static List<LineItem> lineItems = new ArrayList<>();
    static List<Ledger> ledgers = new ArrayList<>();
    static List<LedgerEntry> ledgerEntries = new ArrayList<>();
    static List<TaxRecord> taxRecords = new ArrayList<>();
    static List<PaymentMethod> paymentMethods = new ArrayList<>();
    static List<FinancialReport> financialReports = new ArrayList<>();
    static List<Currency> currencies = new ArrayList<>();

    static List<Project> projects = new ArrayList<>();
    static List<Task> tasks = new ArrayList<>();
    static List<Sprint> sprints = new ArrayList<>();
    static List<Milestone> milestones = new ArrayList<>();
    static List<Release> releases = new ArrayList<>();
    static List<RiskRegister> riskRegisters = new ArrayList<>();
    static List<RiskItem> riskItems = new ArrayList<>();
    static List<Backlog> backlogs = new ArrayList<>();
    static List<Retrospective> retrospectives = new ArrayList<>();
    static List<Dependency> dependencies = new ArrayList<>();
    static List<ProjectMetrics> projectMetricsList = new ArrayList<>();

    static List<Server> servers = new ArrayList<>();
    static List<Network> networks = new ArrayList<>();
    static List<Deployment> deployments = new ArrayList<>();
    static List<Monitor> monitors = new ArrayList<>();
    static List<Alert> alerts = new ArrayList<>();
    static List<LoadBalancer> loadBalancers = new ArrayList<>();
    static List<Database> databases = new ArrayList<>();
    static List<SslCertificate> sslCertificates = new ArrayList<>();
    static List<CloudRegion> cloudRegions = new ArrayList<>();
    static List<Firewall> firewalls = new ArrayList<>();

    static List<Product> products = new ArrayList<>();
    static List<Warehouse> warehouses = new ArrayList<>();
    static List<StockRecord> stockRecords = new ArrayList<>();
    static List<Supplier> suppliers = new ArrayList<>();
    static List<PurchaseOrder> purchaseOrders = new ArrayList<>();
    static List<PurchaseOrderLine> purchaseOrderLines = new ArrayList<>();
    static List<Shipment> shipments = new ArrayList<>();
    static List<Category> categories = new ArrayList<>();
    static List<InventoryAudit> inventoryAudits = new ArrayList<>();
    static List<ReturnRecord> returnRecords = new ArrayList<>();

    static List<Channel> channels = new ArrayList<>();
    static List<Message> messages = new ArrayList<>();
    static List<Attachment> attachments = new ArrayList<>();
    static List<Notification> notifications = new ArrayList<>();
    static List<EmailTemplate> emailTemplates = new ArrayList<>();
    static List<Announcement> announcements = new ArrayList<>();
    static List<ChatRoom> chatRooms = new ArrayList<>();
    static List<SurveyResponse> surveyResponses = new ArrayList<>();
    static List<ContactList> contactLists = new ArrayList<>();

    static List<Report> reports = new ArrayList<>();
    static List<DataPoint> dataPoints = new ArrayList<>();
    static List<Dashboard> dashboards = new ArrayList<>();
    static List<Widget> widgets = new ArrayList<>();
    static List<MetricDefinition> metricDefinitions = new ArrayList<>();
    static List<Trend> trends = new ArrayList<>();
    static List<Anomaly> anomalies = new ArrayList<>();
    static List<ExportJob> exportJobs = new ArrayList<>();

    static List<UserAccount> userAccounts = new ArrayList<>();
    static List<Permission> permissions = new ArrayList<>();
    static List<AuditLog> auditLogs = new ArrayList<>();
    static List<Session> sessions = new ArrayList<>();
    static List<Credential> credentials = new ArrayList<>();
    static List<SecurityPolicy> securityPolicies = new ArrayList<>();
    static List<ThreatEvent> threatEvents = new ArrayList<>();

    public static void main(String[] args) {
        if (args.length > 0) {
            SCALE = Double.parseDouble(args[0]);
        }
        System.out.println("HeapTest Generator - Scale factor: " + SCALE);
        System.out.println("Target instance counts (approximate):");

        long startTime = System.currentTimeMillis();

        createCore();
        System.out.println("[" + elapsed(startTime) + "] Core created");

        createOrg();
        System.out.println("[" + elapsed(startTime) + "] Org created");

        createHR();
        System.out.println("[" + elapsed(startTime) + "] HR created (" + employees.size() + " employees)");

        createFinance();
        System.out.println("[" + elapsed(startTime) + "] Finance created (" + transactions.size() + " transactions)");

        createProjects();
        System.out.println("[" + elapsed(startTime) + "] Projects created (" + tasks.size() + " tasks)");

        createInfra();
        System.out.println("[" + elapsed(startTime) + "] Infra created (" + servers.size() + " servers)");

        createInventory();
        System.out.println("[" + elapsed(startTime) + "] Inventory created (" + products.size() + " products)");

        createComms();
        System.out.println("[" + elapsed(startTime) + "] Comms created (" + messages.size() + " messages)");

        createAnalytics();
        System.out.println("[" + elapsed(startTime) + "] Analytics created (" + dataPoints.size() + " data points)");

        createSecurity();
        System.out.println("[" + elapsed(startTime) + "] Security created (" + auditLogs.size() + " audit logs)");

        wireCrossReferences();
        System.out.println("[" + elapsed(startTime) + "] Cross-references wired");

        printStats();

        // Trigger heap dump
        String dumpPath = "test-heap.hprof";
        System.out.println("\nDumping heap to: " + dumpPath);
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            com.sun.management.HotSpotDiagnosticMXBean bean =
                ManagementFactory.newPlatformMXBeanProxy(mbs,
                    "com.sun.management:type=HotSpotDiagnostic",
                    com.sun.management.HotSpotDiagnosticMXBean.class);
            java.io.File f = new java.io.File(dumpPath);
            if (f.exists()) f.delete();
            bean.dumpHeap(dumpPath, true);
            System.out.println("Heap dump complete: " + new java.io.File(dumpPath).length() / (1024*1024) + " MB");
        } catch (Exception e) {
            System.err.println("Failed to dump heap: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("[" + elapsed(startTime) + "] Total time");
    }

    static String elapsed(long start) {
        long ms = System.currentTimeMillis() - start;
        return String.format("%.1fs", ms / 1000.0);
    }

    static Address randomAddress() {
        return new Address(
            STREETS[RNG.nextInt(STREETS.length)],
            CITIES[RNG.nextInt(CITIES.length)],
            STATES[RNG.nextInt(STATES.length)],
            String.format("%05d", RNG.nextInt(99999)),
            COUNTRIES[RNG.nextInt(COUNTRIES.length)],
            -90 + RNG.nextDouble() * 180,
            -180 + RNG.nextDouble() * 360
        );
    }

    static String randomName() {
        return FIRST_NAMES[RNG.nextInt(FIRST_NAMES.length)];
    }

    static String randomLastName() {
        return LAST_NAMES[RNG.nextInt(LAST_NAMES.length)];
    }

    static byte[] randomBytes(int len) {
        byte[] b = new byte[len];
        RNG.nextBytes(b);
        return b;
    }

    static char[] randomChars(int len) {
        char[] c = new char[len];
        for (int i = 0; i < len; i++) c[i] = (char)('A' + RNG.nextInt(26));
        return c;
    }

    // ======================== CORE ========================

    static void createCore() {
        // Configurations
        for (int i = 0; i < s(100); i++) {
            Configuration c = new Configuration(ID_GEN.nextId(), "config-" + i, 32 + RNG.nextInt(64));
            for (int j = 0; j < 20; j++) {
                c.setProperty("prop." + j, "value-" + RNG.nextInt(10000));
                c.setToggle(j, RNG.nextBoolean());
            }
            configurations.add(c);
        }

        // Addresses
        for (int i = 0; i < s(5000); i++) {
            addresses.add(randomAddress());
        }

        // DateRanges
        long now = System.currentTimeMillis();
        for (int i = 0; i < s(2000); i++) {
            long start = now - RNG.nextInt(365) * 86400000L;
            dateRanges.add(new DateRange(start, start + (30 + RNG.nextInt(335)) * 86400000L, "range-" + i));
        }
    }

    // ======================== ORG ========================

    static void createOrg() {
        // Organizations
        for (int i = 0; i < s(500); i++) {
            Organization org = new Organization(ID_GEN.nextId(), "Org-" + i, "org" + i + ".com");
            org.addTag("industry-" + (i % 10));
            organizations.add(org);
        }

        // Departments
        for (int i = 0; i < s(5000); i++) {
            Department d = new Department(ID_GEN.nextId(), "Dept-" + i, "D" + String.format("%04d", i));
            Organization org = organizations.get(i % organizations.size());
            d.setOrganization(org);
            org.addDepartment(d);
            departments.add(d);
        }

        // Teams
        for (int i = 0; i < s(10000); i++) {
            Team t = new Team(ID_GEN.nextId(), "Team-" + i);
            Department d = departments.get(i % departments.size());
            t.setDepartment(d);
            d.addTeam(t);
            teams.add(t);
        }

        // Divisions
        for (int i = 0; i < s(1000); i++) {
            Division div = new Division(ID_GEN.nextId(), "Division-" + i);
            Organization org = organizations.get(i % organizations.size());
            div.setOrganization(org);
            // Add some departments
            for (int j = 0; j < 3 && (i * 3 + j) < departments.size(); j++) {
                div.addDepartment(departments.get(i * 3 + j));
            }
            divisions.add(div);
        }

        // OrgCharts
        for (int i = 0; i < organizations.size(); i++) {
            OrgChart chart = new OrgChart(ID_GEN.nextId(), organizations.get(i));
            for (int j = 0; j < Math.min(5, divisions.size()); j++) {
                chart.addDivision(divisions.get((i * 5 + j) % divisions.size()));
                chart.addNode("Node-" + i + "-" + j);
            }
            orgCharts.add(chart);
        }

        // Offices
        for (int i = 0; i < s(2000); i++) {
            Office off = new Office(ID_GEN.nextId(), randomAddress(),
                "BLD" + String.format("%03d", i % 999), 50 + RNG.nextInt(950));
            off.setPhoneNumber("+1-555-" + String.format("%04d", RNG.nextInt(9999)));
            if (i < departments.size()) off.addDepartment(departments.get(i));
            offices.add(off);
        }

        // OrgPolicies
        for (int i = 0; i < s(1000); i++) {
            OrgPolicy p = new OrgPolicy(ID_GEN.nextId(), "Policy-" + i,
                "Description for policy " + i + " covering various organizational aspects");
            Organization org = organizations.get(i % organizations.size());
            p.setOrganization(org);
            org.addPolicy(p);
            orgPolicies.add(p);
        }

        // CostCenters
        for (int i = 0; i < s(2000); i++) {
            CostCenter cc = new CostCenter(ID_GEN.nextId(), "CC" + String.format("%05d", i), "CostCenter-" + i);
            cc.setAnnualBudget(100000 + RNG.nextDouble() * 9900000);
            for (int m = 0; m < 12; m++) {
                cc.setMonthlySpending(m, 5000 + RNG.nextDouble() * 500000);
            }
            if (i < departments.size()) {
                cc.addDepartment(departments.get(i));
                departments.get(i).setCostCenter(cc);
            }
            costCenters.add(cc);
        }

        // OrgEvents
        long now = System.currentTimeMillis();
        for (int i = 0; i < s(3000); i++) {
            OrgEvent e = new OrgEvent(ID_GEN.nextId(), "Event-" + i, now + RNG.nextInt(365) * 86400000L);
            e.setDescription("Annual event #" + i);
            e.setLocation(CITIES[RNG.nextInt(CITIES.length)]);
            e.setOrganization(organizations.get(i % organizations.size()));
            orgEvents.add(e);
        }

        // Subsidiaries
        for (int i = 0; i < s(500); i++) {
            Subsidiary sub = new Subsidiary(ID_GEN.nextId(), "Subsidiary-" + i, randomAddress());
            sub.setParentOrg(organizations.get(i % organizations.size()));
            sub.setRevenue(1000000 + RNG.nextDouble() * 99000000);
            sub.setHeadcount(10 + RNG.nextInt(990));
            sub.setCountryCode(COUNTRIES[RNG.nextInt(COUNTRIES.length)]);
            organizations.get(i % organizations.size()).addSubsidiary(sub);
            subsidiaries.add(sub);
        }

        // Committees
        for (int i = 0; i < s(500); i++) {
            Committee c = new Committee(ID_GEN.nextId(), "Committee-" + i);
            c.setCharter("Charter for committee " + i);
            for (int j = 0; j < 5; j++) {
                c.addMeetingDate(now + (j * 30L) * 86400000L);
            }
            committees.add(c);
        }

        // OrgMetrics
        for (int i = 0; i < organizations.size(); i++) {
            OrgMetrics om = new OrgMetrics(ID_GEN.nextId(), organizations.get(i));
            om.setHeadcount(100 + RNG.nextInt(9900));
            om.setRevenue(1000000 + RNG.nextDouble() * 999000000);
            om.setProfitMargin(0.05 + RNG.nextDouble() * 0.35);
            om.setMetric("retention_rate", 0.8 + RNG.nextDouble() * 0.2);
            om.setMetric("growth_rate", RNG.nextDouble() * 0.3);
            om.setMetric("satisfaction", 3.0 + RNG.nextDouble() * 2.0);
            orgMetricsList.add(om);
        }
    }

    // ======================== HR ========================

    static void createHR() {
        // Roles
        String[] roleTitles = {"Engineer","Manager","Director","VP","Analyst","Designer","Architect","Lead","Intern","Consultant"};
        for (int i = 0; i < s(200); i++) {
            Role r = new Role(ID_GEN.nextId(), roleTitles[i % roleTitles.length] + "-" + i, 1 + (i % 10));
            r.setDescription("Role description for " + r.getTitle());
            r.setMinSalary(30000 + (i % 10) * 15000);
            r.setMaxSalary(r.getMinSalary() + 50000);
            r.addPermission("read");
            r.addPermission("write");
            if (i % 3 == 0) r.addPermission("admin");
            roles.add(r);
        }

        // Employees (high volume)
        for (int i = 0; i < s(200000); i++) {
            String fn = randomName();
            String ln = randomLastName();
            Employee emp = new Employee(ID_GEN.nextId(), fn, ln,
                fn.toLowerCase() + "." + ln.toLowerCase() + i + "@example.com",
                "EMP" + String.format("%06d", i));
            emp.setSalary(40000 + RNG.nextDouble() * 160000);
            emp.setRole(roles.get(i % roles.size()));
            emp.setDepartment(departments.get(i % departments.size()));
            emp.setTeam(teams.get(i % teams.size()));
            emp.setPhotoThumbnail(randomBytes(64 + RNG.nextInt(192)));
            emp.setDateOfBirth(System.currentTimeMillis() - (20 + RNG.nextInt(45)) * 365L * 86400000L);
            emp.addTag("dept-" + (i % 20));
            if (i % 7 == 0) emp.addTag("senior");
            employees.add(emp);
        }

        // Wire manager relationships (every employee after the first 100 gets a manager from earlier employees)
        for (int i = 100; i < employees.size(); i++) {
            Employee mgr = employees.get(RNG.nextInt(Math.min(i, 1000)));
            employees.get(i).setManager(mgr);
        }

        // Contractors
        String[] companies = {"Acme Corp","TechStaff Inc","Global Consultants","FlexWork","ProStaff"};
        for (int i = 0; i < s(5000); i++) {
            Contractor c = new Contractor(ID_GEN.nextId(), randomName(), randomLastName(),
                "contractor" + i + "@external.com", companies[i % companies.length]);
            c.setHourlyRate(50 + RNG.nextDouble() * 200);
            c.setMaxHoursPerWeek(20 + RNG.nextInt(25));
            c.setProjectName("Project-" + (i % 100));
            c.setPhotoThumbnail(randomBytes(32 + RNG.nextInt(96)));
            contractors.add(c);
        }

        // Recruiters (extends Employee - deepest inheritance)
        for (int i = 0; i < s(500); i++) {
            String fn = randomName();
            String ln = randomLastName();
            Recruiter r = new Recruiter(ID_GEN.nextId(), fn, ln,
                fn.toLowerCase() + ".recruiter" + i + "@example.com",
                "REC" + String.format("%04d", i));
            r.setSalary(60000 + RNG.nextDouble() * 80000);
            r.setRole(roles.get(0));
            r.addSpecialization("Engineering");
            r.addSpecialization("Product");
            r.setOpenPositions(1 + RNG.nextInt(10));
            r.setPlacementRate(0.2 + RNG.nextDouble() * 0.6);
            // Add some placements
            for (int j = 0; j < 3 && j < employees.size(); j++) {
                r.addPlacement(employees.get(RNG.nextInt(employees.size())));
            }
            recruiters.add(r);
        }

        // Payrolls
        for (int i = 0; i < s(200000); i++) {
            Employee emp = employees.get(i % employees.size());
            Payroll p = new Payroll(ID_GEN.nextId(), emp);
            p.setTaxRate(0.15 + RNG.nextDouble() * 0.25);
            p.setBankAccount("****" + String.format("%04d", RNG.nextInt(9999)));
            long base = (long)(emp.getSalary() / 12);
            for (int m = 0; m < 12; m++) {
                p.setMonthlyGross(m, base + RNG.nextInt(1000));
            }
            p.addDeduction("health", 200 + RNG.nextDouble() * 400);
            p.addDeduction("retirement", 100 + RNG.nextDouble() * 500);
            p.addDeduction("dental", 30 + RNG.nextDouble() * 70);
            payrolls.add(p);
        }

        // Benefits
        String[] healthPlans = {"Basic","Standard","Premium","Platinum"};
        String[] dentalPlans = {"Basic Dental","Full Dental"};
        for (int i = 0; i < s(200000); i++) {
            Benefits b = new Benefits(ID_GEN.nextId(), employees.get(i % employees.size()));
            b.setHealthPlan(healthPlans[RNG.nextInt(healthPlans.length)]);
            b.setDentalPlan(dentalPlans[RNG.nextInt(dentalPlans.length)]);
            b.setVisionPlan(RNG.nextBoolean() ? "Standard Vision" : null);
            b.setRetirementContribution(0.03 + RNG.nextDouble() * 0.12);
            b.setEmployerMatch(0.03 + RNG.nextDouble() * 0.06);
            b.setPtoBalance(10 + RNG.nextInt(25));
            benefitsList.add(b);
        }

        // PerformanceReviews
        for (int i = 0; i < s(100000); i++) {
            Employee emp = employees.get(RNG.nextInt(employees.size()));
            Employee reviewer = employees.get(RNG.nextInt(Math.min(1000, employees.size())));
            PerformanceReview pr = new PerformanceReview(ID_GEN.nextId(), emp, reviewer);
            pr.setRating(1.0f + RNG.nextFloat() * 4.0f);
            for (int c = 0; c < 5; c++) {
                pr.setCategoryScore(c, 1.0f + RNG.nextFloat() * 4.0f);
            }
            pr.setComments("Review comments for employee " + emp.getEmployeeId() + " - " + RNG.nextInt(10000));
            pr.setPeriod("Q" + (1 + RNG.nextInt(4)) + "-202" + RNG.nextInt(6));
            pr.setFinalized(RNG.nextBoolean());
            reviews.add(pr);
        }

        // Attendance (very high volume)
        for (int i = 0; i < s(1500000); i++) {
            Employee emp = employees.get(RNG.nextInt(employees.size()));
            long date = System.currentTimeMillis() - RNG.nextInt(365) * 86400000L;
            Attendance a = new Attendance(ID_GEN.nextId(), emp, date);
            a.setHoursWorked(4.0f + RNG.nextFloat() * 8.0f);
            a.setOvertime(RNG.nextFloat() > 0.85f);
            a.setRemote(RNG.nextBoolean());
            if (RNG.nextFloat() > 0.9f) a.setNotes("Note: " + RNG.nextInt(10000));
            attendances.add(a);
        }

        // TrainingRecords
        String[] courses = {"Java Basics","Cloud Architecture","Leadership","Security 101","Agile","Data Science",
            "ML Fundamentals","Communication","Project Mgmt","DevOps"};
        for (int i = 0; i < s(10000); i++) {
            TrainingRecord tr = new TrainingRecord(ID_GEN.nextId(),
                employees.get(RNG.nextInt(employees.size())),
                courses[RNG.nextInt(courses.length)]);
            tr.setProvider("TrainingCo-" + (i % 5));
            tr.setCompletionDate(System.currentTimeMillis() - RNG.nextInt(365) * 86400000L);
            tr.setScore(50 + RNG.nextInt(51));
            tr.setCertified(tr.getScore() >= 70);
            tr.setCost(100 + RNG.nextDouble() * 4900);
            trainingRecords.add(tr);
        }

        // LeaveRequests
        String[] leaveTypes = {"VACATION","SICK","PERSONAL","PARENTAL"};
        for (int i = 0; i < s(20000); i++) {
            long start = System.currentTimeMillis() + RNG.nextInt(180) * 86400000L;
            LeaveRequest lr = new LeaveRequest(ID_GEN.nextId(),
                employees.get(RNG.nextInt(employees.size())),
                start, start + (1 + RNG.nextInt(14)) * 86400000L,
                leaveTypes[RNG.nextInt(leaveTypes.length)]);
            lr.setApproved(RNG.nextFloat() > 0.2f);
            if (lr.isApproved()) {
                lr.setApprovedBy(employees.get(RNG.nextInt(Math.min(1000, employees.size()))));
            }
            lr.setReason("Reason for leave #" + i);
            leaveRequests.add(lr);
        }

        // EmployeeDirectory
        EmployeeDirectory dir = new EmployeeDirectory(ID_GEN.nextId());
        for (Employee emp : employees) {
            dir.addEmployee(emp);
        }
        // Index some employees by department
        for (int i = 0; i < Math.min(10000, employees.size()); i++) {
            Employee emp = employees.get(i);
            dir.indexByDepartment("Dept-" + (i % departments.size()), emp);
        }
        directories.add(dir);
    }

    // ======================== FINANCE ========================

    static void createFinance() {
        // Currencies
        String[][] currData = {{"USD","US Dollar",'$'+"","1.0"},{"EUR","Euro",'E'+"","0.92"},
            {"GBP","British Pound",'P'+"","0.79"},{"JPY","Japanese Yen",'Y'+"","149.5"},
            {"CAD","Canadian Dollar",'C'+"","1.36"}};
        for (String[] cd : currData) {
            Currency c = new Currency(ID_GEN.nextId(), cd[0], cd[1], cd[2].charAt(0));
            c.setExchangeRate(Double.parseDouble(cd[3]));
            currencies.add(c);
        }

        // PaymentMethods
        String[] pmTypes = {"CREDIT_CARD","WIRE","ACH","CHECK"};
        for (int i = 0; i < s(5000); i++) {
            PaymentMethod pm = new PaymentMethod(ID_GEN.nextId(),
                pmTypes[RNG.nextInt(pmTypes.length)],
                String.format("%04d", RNG.nextInt(9999)));
            pm.setActive(RNG.nextFloat() > 0.1f);
            pm.setBillingAddress(STREETS[RNG.nextInt(STREETS.length)] + ", " + CITIES[RNG.nextInt(CITIES.length)]);
            paymentMethods.add(pm);
        }

        // Accounts
        String[] acctTypes = {"CHECKING","SAVINGS","EXPENSE","REVENUE"};
        for (int i = 0; i < s(10000); i++) {
            Account a = new Account(ID_GEN.nextId(),
                "ACCT" + String.format("%08d", i),
                acctTypes[RNG.nextInt(acctTypes.length)]);
            a.setBalance(RNG.nextDouble() * 1000000);
            if (i < employees.size()) a.setOwner(employees.get(i));
            if (!currencies.isEmpty()) a.setCurrency(currencies.get(RNG.nextInt(currencies.size())));
            accounts.add(a);
        }

        // Transactions (very high volume)
        for (int i = 0; i < s(1500000); i++) {
            Account from = accounts.get(RNG.nextInt(accounts.size()));
            Account to = accounts.get(RNG.nextInt(accounts.size()));
            Transaction t = new Transaction(ID_GEN.nextId(), from, to, 0.01 + RNG.nextDouble() * 50000);
            t.setDescription("Transaction " + i + " - " + (RNG.nextBoolean() ? "payment" : "transfer"));
            t.setReferenceNumber("REF" + String.format("%010d", i));
            t.setReconciled(RNG.nextFloat() > 0.3f);
            if (!currencies.isEmpty()) t.setCurrency(currencies.get(RNG.nextInt(currencies.size())));
            transactions.add(t);
        }

        // Budgets
        for (int i = 0; i < s(2000); i++) {
            Budget b = new Budget(ID_GEN.nextId(), "Budget-" + i, 2024 + RNG.nextInt(3));
            b.setTotalBudget(50000 + RNG.nextDouble() * 9950000);
            for (int m = 0; m < 12; m++) {
                b.setMonthlyAllocation(m, b.getTotalBudget() / 12 * (0.8 + RNG.nextDouble() * 0.4));
            }
            if (i < departments.size()) b.setDepartment(departments.get(i));
            budgets.add(b);
        }

        // LineItems
        for (int i = 0; i < s(200000); i++) {
            LineItem li = new LineItem(ID_GEN.nextId(),
                "Item-" + i + "-" + (RNG.nextBoolean() ? "supplies" : "services"),
                1 + RNG.nextInt(100), 1.0 + RNG.nextDouble() * 999);
            li.setTaxRate(RNG.nextFloat() > 0.5f ? 0.08 : 0.0);
            li.setCategory("Category-" + (i % 20));
            lineItems.add(li);
        }

        // Invoices
        for (int i = 0; i < s(10000); i++) {
            Invoice inv = new Invoice(ID_GEN.nextId(),
                "INV" + String.format("%08d", i),
                "Vendor-" + (i % 200));
            // Add 1-5 line items
            int numLines = 1 + RNG.nextInt(5);
            for (int j = 0; j < numLines && (i * 5 + j) < lineItems.size(); j++) {
                inv.addLineItem(lineItems.get(i * 5 + j));
            }
            inv.setPaid(RNG.nextFloat() > 0.3f);
            if (!paymentMethods.isEmpty()) {
                inv.setPaymentMethod(paymentMethods.get(RNG.nextInt(paymentMethods.size())));
            }
            invoices.add(inv);
        }

        // Ledgers
        for (int i = 0; i < s(500); i++) {
            Ledger l = new Ledger(ID_GEN.nextId(), "Ledger-" + i);
            int numAccounts = Math.min(20, accounts.size());
            for (int j = 0; j < numAccounts; j++) {
                l.addAccount(accounts.get((i * 20 + j) % accounts.size()));
            }
            ledgers.add(l);
        }

        // LedgerEntries (high volume)
        String[] entryTypes = {"DEBIT","CREDIT"};
        for (int i = 0; i < s(1000000); i++) {
            Account acct = accounts.get(RNG.nextInt(accounts.size()));
            LedgerEntry le = new LedgerEntry(ID_GEN.nextId(), acct,
                0.01 + RNG.nextDouble() * 10000, entryTypes[RNG.nextInt(2)]);
            le.setDescription("Ledger entry " + i);
            le.setBalanceAfter(acct.getBalance() + le.getAmount());
            if (!ledgers.isEmpty()) {
                ledgers.get(i % ledgers.size()).addEntry(le);
            }
            ledgerEntries.add(le);
        }

        // TaxRecords
        for (int i = 0; i < s(20000); i++) {
            TaxRecord tr = new TaxRecord(ID_GEN.nextId(),
                employees.get(RNG.nextInt(employees.size())),
                2020 + RNG.nextInt(6));
            tr.setTotalIncome(30000 + RNG.nextDouble() * 170000);
            tr.setFederalTax(tr.getTotalIncome() * (0.1 + RNG.nextDouble() * 0.25));
            tr.setStateTax(tr.getTotalIncome() * (0.02 + RNG.nextDouble() * 0.1));
            tr.setTotalTax(tr.getFederalTax() + tr.getStateTax());
            tr.setFiled(RNG.nextFloat() > 0.1f);
            taxRecords.add(tr);
        }

        // FinancialReports
        for (int i = 0; i < s(1000); i++) {
            DateRange dr = dateRanges.get(i % dateRanges.size());
            FinancialReport fr = new FinancialReport(ID_GEN.nextId(), "Report-" + i, dr);
            fr.setType(i % 3 == 0 ? "QUARTERLY" : i % 3 == 1 ? "ANNUAL" : "MONTHLY");
            fr.addData("revenue", 100000 + RNG.nextDouble() * 9900000);
            fr.addData("expenses", 50000 + RNG.nextDouble() * 4950000);
            fr.addData("profit", RNG.nextDouble() * 5000000);
            fr.addData("headcount", 10 + RNG.nextInt(990));
            fr.setPublished(RNG.nextBoolean());
            financialReports.add(fr);
        }
    }

    // ======================== PROJECTS ========================

    static void createProjects() {
        // Projects
        for (int i = 0; i < s(2000); i++) {
            Project p = new Project(ID_GEN.nextId(), "Project-" + i, "PRJ" + String.format("%04d", i));
            if (!teams.isEmpty()) p.setTeam(teams.get(i % teams.size()));
            p.addTag("priority-" + (i % 5));
            if (i % 10 == 0) p.setStatus(CoreEnums.Status.ARCHIVED);
            projects.add(p);
        }

        // Tasks (high volume, self-referencing)
        String[] taskTypes = {"BUG","FEATURE","CHORE","SPIKE"};
        CoreEnums.Priority[] priorities = CoreEnums.Priority.values();
        CoreEnums.Status[] statuses = CoreEnums.Status.values();
        for (int i = 0; i < s(400000); i++) {
            Task t = new Task(ID_GEN.nextId(), "Task-" + i + "-" + taskTypes[i % taskTypes.length]);
            t.setDescription("Description for task " + i + " with details about implementation requirements");
            t.setPriority(priorities[RNG.nextInt(priorities.length)]);
            t.setStatus(statuses[RNG.nextInt(statuses.length)]);
            t.setStoryPoints(1 + RNG.nextInt(13));
            t.setType(taskTypes[i % taskTypes.length]);
            if (!employees.isEmpty()) t.setAssignee(employees.get(RNG.nextInt(employees.size())));
            tasks.add(t);
        }

        // Wire parent-child task relationships (self-reference)
        for (int i = 1000; i < tasks.size(); i += 3) {
            Task parent = tasks.get(RNG.nextInt(Math.min(i, 1000)));
            parent.addSubTask(tasks.get(i));
        }

        // Sprints
        for (int i = 0; i < s(5000); i++) {
            Project p = projects.get(i % projects.size());
            Sprint sp = new Sprint(ID_GEN.nextId(), "Sprint-" + i, p);
            sp.setVelocity(20 + RNG.nextInt(60));
            sp.setCompleted(RNG.nextFloat() > 0.3f);
            p.addSprint(sp);
            // Add tasks to sprint
            int numTasks = 5 + RNG.nextInt(15);
            for (int j = 0; j < numTasks; j++) {
                int tIdx = (i * 20 + j) % tasks.size();
                sp.addTask(tasks.get(tIdx));
            }
            sprints.add(sp);
        }

        // Milestones
        long now = System.currentTimeMillis();
        for (int i = 0; i < s(5000); i++) {
            Project p = projects.get(i % projects.size());
            Milestone m = new Milestone(ID_GEN.nextId(), "Milestone-" + i, p,
                now + (30 + RNG.nextInt(335)) * 86400000L);
            m.setCompleted(RNG.nextFloat() > 0.6f);
            m.setDescription("Milestone " + i + " deliverables");
            p.addMilestone(m);
            milestones.add(m);
        }

        // Releases
        for (int i = 0; i < s(2000); i++) {
            Project p = projects.get(i % projects.size());
            Release r = new Release(ID_GEN.nextId(), (i / 10 + 1) + "." + (i % 10) + ".0", p);
            r.setReleaseDate(now - RNG.nextInt(365) * 86400000L);
            r.setReleaseNotes("Release notes for version " + r.getReleaseVersion());
            r.setDeployed(RNG.nextFloat() > 0.4f);
            for (int j = 0; j < 20; j++) {
                r.setBuildMetric(j, (short)(RNG.nextInt(1000)));
            }
            if (!milestones.isEmpty()) {
                r.addMilestone(milestones.get(i % milestones.size()));
            }
            releases.add(r);
        }

        // Backlogs (uses LinkedList)
        for (int i = 0; i < projects.size(); i++) {
            Project p = projects.get(i);
            Backlog b = new Backlog(ID_GEN.nextId(), p);
            int numItems = 10 + RNG.nextInt(40);
            for (int j = 0; j < numItems; j++) {
                int tIdx = (i * 50 + j) % tasks.size();
                b.addItem(tasks.get(tIdx));
            }
            b.setPrioritized(RNG.nextBoolean());
            p.setBacklog(b);
            backlogs.add(b);
        }

        // RiskRegisters & RiskItems
        for (int i = 0; i < s(1000); i++) {
            Project p = projects.get(i % projects.size());
            RiskRegister rr = new RiskRegister(ID_GEN.nextId(), p);
            int numRisks = 2 + RNG.nextInt(8);
            for (int j = 0; j < numRisks; j++) {
                RiskItem ri = new RiskItem(ID_GEN.nextId(),
                    "Risk-" + i + "-" + j + ": potential issue with deliverables",
                    RNG.nextFloat(), RNG.nextFloat());
                ri.setCategory("Category-" + (j % 5));
                ri.setMitigation("Mitigation plan for risk " + j);
                rr.addItem(ri);
                riskItems.add(ri);
            }
            riskRegisters.add(rr);
        }

        // Retrospectives
        for (int i = 0; i < sprints.size(); i++) {
            if (sprints.get(i).isCompleted()) {
                Retrospective ret = new Retrospective(ID_GEN.nextId(), sprints.get(i));
                ret.addGoodItem("Good collaboration");
                ret.addGoodItem("On-time delivery");
                ret.addBadItem("Technical debt");
                ret.addBadItem("Scope creep");
                ret.addActionItem("Improve CI/CD pipeline");
                ret.addActionItem("Add more tests");
                ret.setParticipantCount(3 + RNG.nextInt(12));
                retrospectives.add(ret);
            }
        }

        // Dependencies
        for (int i = 0; i < s(20000); i++) {
            Task from = tasks.get(RNG.nextInt(tasks.size()));
            Task to = tasks.get(RNG.nextInt(tasks.size()));
            String[] depTypes = {"BLOCKS","DEPENDS_ON","RELATED"};
            Dependency dep = new Dependency(ID_GEN.nextId(), from, to, depTypes[RNG.nextInt(depTypes.length)]);
            dep.setCritical(RNG.nextFloat() > 0.8f);
            dep.setNotes("Dependency note " + i);
            dependencies.add(dep);
        }

        // ProjectMetrics
        for (int i = 0; i < projects.size(); i++) {
            Project p = projects.get(i);
            int sprintCount = Math.max(1, p.getSprints().size());
            ProjectMetrics pm = new ProjectMetrics(ID_GEN.nextId(), p, sprintCount);
            for (int j = 0; j < sprintCount * 10 && j < pm.getBurndownData().length; j++) {
                pm.setBurndownPoint(j, 100 - (j * 100.0 / (sprintCount * 10)));
            }
            for (int j = 0; j < sprintCount && j < pm.getVelocityHistory().length; j++) {
                pm.setVelocity(j, 20 + RNG.nextInt(60));
            }
            pm.setTotalTasks(10 + RNG.nextInt(200));
            pm.setCompletedTasks(RNG.nextInt(pm.getTotalTasks()));
            pm.setCompletionPercentage((double) pm.getCompletedTasks() / pm.getTotalTasks() * 100);
            projectMetricsList.add(pm);
        }
    }

    // ======================== INFRA ========================

    static void createInfra() {
        // Servers
        String[] osTypes = {"Ubuntu 22.04","CentOS 8","Amazon Linux 2","Debian 12","RHEL 9"};
        for (int i = 0; i < s(5000); i++) {
            Server s = new Server(ID_GEN.nextId(),
                "srv-" + String.format("%04d", i) + ".example.com",
                "10." + (i / 65536 % 256) + "." + (i / 256 % 256) + "." + (i % 256),
                2 + RNG.nextInt(62), 4 + RNG.nextInt(252));
            s.setOs(osTypes[RNG.nextInt(osTypes.length)]);
            s.setCpuUtilization(RNG.nextDouble() * 100);
            servers.add(s);
        }

        // Networks
        for (int i = 0; i < s(500); i++) {
            Network n = new Network(ID_GEN.nextId(), "net-" + i,
                "10." + i + ".0.0/16", 100 + i);
            n.setEncrypted(RNG.nextBoolean());
            n.setGateway("10." + i + ".0.1");
            int numServers = 5 + RNG.nextInt(15);
            for (int j = 0; j < numServers; j++) {
                n.addServer(servers.get((i * 10 + j) % servers.size()));
            }
            networks.add(n);
        }

        // Deployments
        for (int i = 0; i < s(10000); i++) {
            Server srv = servers.get(RNG.nextInt(servers.size()));
            Deployment d = new Deployment(ID_GEN.nextId(), srv, "1." + RNG.nextInt(100) + "." + RNG.nextInt(999));
            d.setDeployedBy("deployer-" + (i % 50));
            String[] envs = {"PROD","STAGING","DEV","QA"};
            d.setEnvironment(envs[RNG.nextInt(envs.length)]);
            if (!projects.isEmpty()) d.setProject(projects.get(RNG.nextInt(projects.size())));
            srv.addDeployment(d);
            deployments.add(d);
        }

        // Monitors
        String[] metricNames = {"cpu_usage","memory_usage","disk_io","network_in","network_out","latency","error_rate"};
        for (int i = 0; i < s(10000); i++) {
            Server srv = servers.get(i % servers.size());
            Monitor m = new Monitor(ID_GEN.nextId(), srv,
                metricNames[RNG.nextInt(metricNames.length)], 60 + RNG.nextInt(240));
            m.setThreshold(50 + RNG.nextDouble() * 50);
            m.setCheckIntervalSeconds(30 + RNG.nextInt(270));
            // Fill recent values
            for (int j = 0; j < m.getRecentValues().length; j++) {
                m.recordValue(j, RNG.nextFloat() * 100);
            }
            m.setAlerting(RNG.nextFloat() > 0.9f);
            monitors.add(m);
        }

        // Alerts
        CoreEnums.Severity[] severities = CoreEnums.Severity.values();
        for (int i = 0; i < s(20000); i++) {
            Monitor mon = monitors.get(RNG.nextInt(monitors.size()));
            Alert a = new Alert(ID_GEN.nextId(), mon,
                severities[RNG.nextInt(severities.length)],
                "Alert: " + mon.getMetricName() + " exceeded threshold on " + mon.getTarget().getHostname());
            a.setAcknowledged(RNG.nextFloat() > 0.4f);
            if (a.isAcknowledged()) a.setAcknowledgedBy("oncall-" + (i % 20));
            mon.addAlert(a);
            alerts.add(a);
        }

        // LoadBalancers
        String[] lbAlgorithms = {"ROUND_ROBIN","LEAST_CONNECTIONS","WEIGHTED"};
        for (int i = 0; i < s(500); i++) {
            LoadBalancer lb = new LoadBalancer(ID_GEN.nextId(), "lb-" + i,
                lbAlgorithms[RNG.nextInt(lbAlgorithms.length)]);
            lb.setPort(443 + (i % 10 == 0 ? -363 : 0)); // mostly 443, some 80
            int numServers = 2 + RNG.nextInt(8);
            for (int j = 0; j < numServers; j++) {
                lb.addServer(servers.get((i * 10 + j) % servers.size()));
            }
            for (int j = 0; j < lb.getServers().size(); j++) {
                lb.setConnectionCount(j, RNG.nextInt(5000));
            }
            loadBalancers.add(lb);
        }

        // Databases
        String[] engines = {"POSTGRESQL","MYSQL","MONGODB","REDIS"};
        for (int i = 0; i < s(2000); i++) {
            Database db = new Database(ID_GEN.nextId(), "db-" + i,
                servers.get(i % servers.size()), engines[RNG.nextInt(engines.length)]);
            db.setSizeGb(1 + RNG.nextDouble() * 999);
            db.setMaxConnections(50 + RNG.nextInt(450));
            db.setActiveConnections(RNG.nextInt(db.getMaxConnections()));
            databases.add(db);
        }
        // Wire replicas
        for (int i = 1; i < databases.size(); i += 3) {
            databases.get(i - 1).addReplica(databases.get(i));
        }

        // SslCertificates
        for (int i = 0; i < s(2000); i++) {
            SslCertificate cert = new SslCertificate(ID_GEN.nextId(),
                "*.domain" + i + ".com", randomBytes(32));
            cert.setIssuer("Let's Encrypt");
            cert.setAlgorithm("RSA-SHA256");
            cert.setWildcard(RNG.nextBoolean());
            sslCertificates.add(cert);
        }

        // CloudRegions
        String[][] regionData = {{"US East","us-east-1"},{"US West","us-west-2"},{"EU West","eu-west-1"},
            {"AP Southeast","ap-southeast-1"},{"AP Northeast","ap-northeast-1"}};
        for (int i = 0; i < s(50); i++) {
            String[] rd = regionData[i % regionData.length];
            CloudRegion cr = new CloudRegion(ID_GEN.nextId(), rd[0] + "-" + i, rd[1] + "-" + i);
            cr.setProvider(i % 3 == 0 ? "AWS" : i % 3 == 1 ? "GCP" : "AZURE");
            cr.setAvailability(99.9 + RNG.nextDouble() * 0.09);
            cr.setAvailabilityZones(2 + RNG.nextInt(4));
            int numServers = 10 + RNG.nextInt(90);
            for (int j = 0; j < numServers && j < servers.size(); j++) {
                cr.addServer(servers.get((i * 100 + j) % servers.size()));
            }
            cloudRegions.add(cr);
        }

        // Firewalls
        for (int i = 0; i < s(500); i++) {
            Firewall fw = new Firewall(ID_GEN.nextId(), "fw-" + i,
                networks.get(i % networks.size()));
            fw.setDefaultPolicy(RNG.nextBoolean() ? "DENY" : "ALLOW");
            int numRules = 5 + RNG.nextInt(45);
            for (int j = 0; j < numRules; j++) {
                fw.addRule("ALLOW TCP " + (1024 + RNG.nextInt(64511)) + " FROM 10.0.0.0/8");
            }
            firewalls.add(fw);
        }
    }

    // ======================== INVENTORY ========================

    static void createInventory() {
        // Categories (self-referencing tree)
        String[] catNames = {"Electronics","Clothing","Food","Books","Tools","Furniture","Sports","Toys","Health","Office"};
        for (int i = 0; i < s(200); i++) {
            Category c = new Category(ID_GEN.nextId(), catNames[i % catNames.length] + "-" + i);
            categories.add(c);
        }
        // Wire parent-child
        for (int i = 10; i < categories.size(); i++) {
            categories.get(i).setParentCategory(categories.get(i % 10));
        }

        // Products
        for (int i = 0; i < s(20000); i++) {
            Product p = new Product(ID_GEN.nextId(), "Product-" + i,
                "SKU" + String.format("%08d", i), 0.99 + RNG.nextDouble() * 999);
            p.setWeight(0.1f + RNG.nextFloat() * 49.9f);
            p.setDescription("Product description for " + p.getName() + " with various specifications");
            p.setActive(RNG.nextFloat() > 0.05f);
            if (!categories.isEmpty()) {
                Category cat = categories.get(i % categories.size());
                p.setCategory(cat);
                cat.addProduct(p);
            }
            p.addTag("category-" + (i % 10));
            products.add(p);
        }

        // Suppliers
        for (int i = 0; i < s(1000); i++) {
            Supplier s = new Supplier(ID_GEN.nextId(), "Supplier-" + i, "contact@supplier" + i + ".com");
            s.setAddress(randomAddress());
            s.setRating(1.0f + RNG.nextFloat() * 4.0f);
            s.setPhone("+1-555-" + String.format("%04d", RNG.nextInt(9999)));
            // Add some products
            int numProducts = 5 + RNG.nextInt(20);
            for (int j = 0; j < numProducts && j < products.size(); j++) {
                s.addProduct(products.get((i * 20 + j) % products.size()));
            }
            suppliers.add(s);
        }

        // Warehouses
        for (int i = 0; i < s(1000); i++) {
            Warehouse w = new Warehouse(ID_GEN.nextId(), "Warehouse-" + i,
                randomAddress(), 1000 + RNG.nextInt(99000));
            if (!employees.isEmpty()) w.setManager(employees.get(RNG.nextInt(employees.size())));
            warehouses.add(w);
        }

        // StockRecords
        for (int i = 0; i < s(200000); i++) {
            Product p = products.get(i % products.size());
            Warehouse w = warehouses.get(i % warehouses.size());
            StockRecord sr = new StockRecord(ID_GEN.nextId(), p, w, RNG.nextInt(10000));
            sr.setMinQuantity(5 + RNG.nextInt(95));
            sr.setLocation("A" + (i % 26) + "-S" + (i % 10) + "-B" + (i % 50));
            w.addStockRecord(sr);
            stockRecords.add(sr);
        }

        // PurchaseOrders & PurchaseOrderLines
        for (int i = 0; i < s(5000); i++) {
            Supplier sup = suppliers.get(i % suppliers.size());
            PurchaseOrder po = new PurchaseOrder(ID_GEN.nextId(), sup);
            po.setExpectedDelivery(System.currentTimeMillis() + (7 + RNG.nextInt(23)) * 86400000L);
            CoreEnums.Status[] poStatuses = {CoreEnums.Status.PENDING, CoreEnums.Status.ACTIVE, CoreEnums.Status.ARCHIVED};
            po.setStatus(poStatuses[RNG.nextInt(poStatuses.length)]);
            int numLines = 1 + RNG.nextInt(10);
            for (int j = 0; j < numLines; j++) {
                Product p = products.get(RNG.nextInt(products.size()));
                PurchaseOrderLine line = new PurchaseOrderLine(ID_GEN.nextId(), p,
                    1 + RNG.nextInt(500), p.getPrice() * (0.7 + RNG.nextDouble() * 0.3));
                po.addLine(line);
                purchaseOrderLines.add(line);
            }
            purchaseOrders.add(po);
        }

        // Shipments
        for (int i = 0; i < s(5000); i++) {
            PurchaseOrder po = purchaseOrders.get(i % purchaseOrders.size());
            Shipment sh = new Shipment(ID_GEN.nextId(), po, "TRK" + String.format("%012d", i));
            sh.setOrigin(randomAddress());
            sh.setDestination(randomAddress());
            sh.setWeight(1.0 + RNG.nextDouble() * 999);
            String[] carriers = {"FedEx","UPS","DHL","USPS"};
            sh.setCarrier(carriers[RNG.nextInt(carriers.length)]);
            CoreEnums.Status[] shStatuses = {CoreEnums.Status.PENDING, CoreEnums.Status.ACTIVE, CoreEnums.Status.ARCHIVED};
            sh.setStatus(shStatuses[RNG.nextInt(shStatuses.length)]);
            shipments.add(sh);
        }

        // InventoryAudits
        for (int i = 0; i < s(2000); i++) {
            InventoryAudit ia = new InventoryAudit(ID_GEN.nextId(),
                warehouses.get(i % warehouses.size()));
            if (!employees.isEmpty()) ia.setAuditor(employees.get(RNG.nextInt(employees.size())));
            ia.setDiscrepancies(RNG.nextInt(50));
            ia.setNotes("Audit notes for warehouse " + (i % warehouses.size()));
            ia.setCompleted(RNG.nextFloat() > 0.3f);
            inventoryAudits.add(ia);
        }

        // ReturnRecords
        for (int i = 0; i < s(5000); i++) {
            Product p = products.get(RNG.nextInt(products.size()));
            String[] reasons = {"Defective","Wrong item","Not as described","Changed mind","Damaged in shipping"};
            ReturnRecord rr = new ReturnRecord(ID_GEN.nextId(), p,
                reasons[RNG.nextInt(reasons.length)], 1 + RNG.nextInt(5));
            String[] conditions = {"NEW","USED","DAMAGED"};
            rr.setCondition(conditions[RNG.nextInt(conditions.length)]);
            rr.setProcessed(RNG.nextFloat() > 0.3f);
            returnRecords.add(rr);
        }
    }

    // ======================== COMMS ========================

    static void createComms() {
        // Channels
        String[] channelTypes = {"EMAIL","CHAT","SMS","SLACK"};
        for (int i = 0; i < s(2000); i++) {
            Channel ch = new Channel(ID_GEN.nextId(), "channel-" + i,
                channelTypes[RNG.nextInt(channelTypes.length)]);
            ch.setArchived(RNG.nextFloat() > 0.9f);
            int numMembers = 2 + RNG.nextInt(48);
            for (int j = 0; j < numMembers && j < employees.size(); j++) {
                ch.addMember(employees.get((i * 50 + j) % employees.size()));
            }
            channels.add(ch);
        }

        // Messages (high volume, self-referencing)
        for (int i = 0; i < s(1000000); i++) {
            Channel ch = channels.get(RNG.nextInt(channels.size()));
            Object sender = employees.isEmpty() ? null : employees.get(RNG.nextInt(employees.size()));
            Message m = new Message(ID_GEN.nextId(), sender, ch,
                "Message content " + i + ": Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed " + RNG.nextInt(100000));
            ch.addMessage(m);
            messages.add(m);
        }
        // Wire parent messages (self-reference)
        for (int i = 1000; i < messages.size(); i += 5) {
            messages.get(i).setParentMessage(messages.get(RNG.nextInt(Math.min(i, 1000))));
        }

        // Attachments
        for (int i = 0; i < s(10000); i++) {
            String[] mimeTypes = {"text/plain","image/png","application/pdf","image/jpeg","application/json"};
            Attachment a = new Attachment(ID_GEN.nextId(),
                "file-" + i + ".dat", mimeTypes[RNG.nextInt(mimeTypes.length)],
                randomBytes(32 + RNG.nextInt(224)));
            a.setChecksum("sha256:" + Long.toHexString(RNG.nextLong()));
            // Attach to messages
            if (!messages.isEmpty()) {
                messages.get(i % messages.size()).addAttachment(a);
            }
            attachments.add(a);
        }

        // Notifications (high volume)
        String[] notifTypes = {"INFO","WARNING","ACTION_REQUIRED"};
        for (int i = 0; i < s(800000); i++) {
            Object recipient = employees.isEmpty() ? null : employees.get(RNG.nextInt(employees.size()));
            Notification n = new Notification(ID_GEN.nextId(), recipient,
                "Notification " + i, "Body of notification " + i + " with details");
            n.setRead(RNG.nextFloat() > 0.4f);
            n.setType(notifTypes[RNG.nextInt(notifTypes.length)]);
            if (!channels.isEmpty()) n.setChannel(channels.get(RNG.nextInt(channels.size())));
            notifications.add(n);
        }

        // EmailTemplates
        for (int i = 0; i < s(500); i++) {
            EmailTemplate et = new EmailTemplate(ID_GEN.nextId(),
                "Template-" + i, "Subject: {{topic}} - " + i,
                "Dear {{name}},\n\nThis is a {{type}} email regarding {{topic}}.\n\nBest,\n{{sender}}");
            et.addVariable("name");
            et.addVariable("topic");
            et.addVariable("type");
            et.addVariable("sender");
            et.setActive(RNG.nextFloat() > 0.1f);
            emailTemplates.add(et);
        }

        // Announcements
        for (int i = 0; i < s(2000); i++) {
            Announcement ann = new Announcement(ID_GEN.nextId(),
                "Announcement " + i, "Important announcement body " + i + " with organizational updates");
            if (!employees.isEmpty()) ann.setAuthor(employees.get(RNG.nextInt(employees.size())));
            ann.addAudience("All Employees");
            ann.addAudience("Department-" + (i % 10));
            ann.setPriority(CoreEnums.Priority.values()[RNG.nextInt(CoreEnums.Priority.values().length)]);
            announcements.add(ann);
        }

        // ChatRooms (uses ArrayDeque)
        for (int i = 0; i < s(1000); i++) {
            ChatRoom cr = new ChatRoom(ID_GEN.nextId(), "room-" + i, 50 + RNG.nextInt(450));
            cr.setPrivate(RNG.nextBoolean());
            cr.setTopic("Discussion topic " + i);
            int numParticipants = 2 + RNG.nextInt(18);
            for (int j = 0; j < numParticipants && j < employees.size(); j++) {
                cr.addParticipant(employees.get((i * 20 + j) % employees.size()));
            }
            // Add some messages
            int numMsgs = 5 + RNG.nextInt(20);
            for (int j = 0; j < numMsgs && j < messages.size(); j++) {
                cr.addMessage(messages.get((i * 25 + j) % messages.size()));
            }
            chatRooms.add(cr);
        }

        // SurveyResponses
        for (int i = 0; i < s(10000); i++) {
            Object respondent = employees.isEmpty() ? null : employees.get(RNG.nextInt(employees.size()));
            String[] surveyNames = {"Engagement Survey","Feedback Survey","Exit Survey","Onboarding Survey"};
            SurveyResponse sr = new SurveyResponse(ID_GEN.nextId(), respondent,
                surveyNames[RNG.nextInt(surveyNames.length)]);
            sr.addAnswer("Q1: How satisfied are you?", String.valueOf(1 + RNG.nextInt(5)));
            sr.addAnswer("Q2: Would you recommend?", RNG.nextBoolean() ? "Yes" : "No");
            sr.addAnswer("Q3: Comments", "Comment-" + RNG.nextInt(10000));
            sr.setAnonymous(RNG.nextBoolean());
            surveyResponses.add(sr);
        }

        // ContactLists (uses LinkedList)
        for (int i = 0; i < s(2000); i++) {
            ContactList cl = new ContactList(ID_GEN.nextId(), "contacts-" + i);
            if (!employees.isEmpty()) {
                cl.setOwner(employees.get(i % employees.size()));
                int numContacts = 5 + RNG.nextInt(45);
                for (int j = 0; j < numContacts; j++) {
                    cl.addContact(employees.get(RNG.nextInt(employees.size())));
                }
            }
            cl.setDescription("Contact list " + i);
            cl.setShared(RNG.nextBoolean());
            contactLists.add(cl);
        }
    }

    // ======================== ANALYTICS ========================

    static void createAnalytics() {
        // MetricDefinitions
        String[][] metricDefs = {
            {"revenue","USD","SUM"},{"page_views","count","SUM"},{"response_time","ms","AVG"},
            {"error_rate","percent","AVG"},{"cpu_usage","percent","AVG"},{"conversion","percent","AVG"},
            {"latency","ms","AVG"},{"throughput","req/s","AVG"},{"availability","percent","MIN"},
            {"satisfaction","score","AVG"}
        };
        for (int i = 0; i < s(200); i++) {
            String[] md = metricDefs[i % metricDefs.length];
            MetricDefinition mDef = new MetricDefinition(ID_GEN.nextId(),
                md[0] + "-" + i, md[1], md[2]);
            mDef.setDescription("Metric definition for " + md[0]);
            metricDefinitions.add(mDef);
        }

        // DataPoints (very high volume)
        long now = System.currentTimeMillis();
        for (int i = 0; i < s(1500000); i++) {
            DataPoint dp = new DataPoint(ID_GEN.nextId(),
                metricDefinitions.get(i % metricDefinitions.size()).getName(),
                RNG.nextDouble() * 10000,
                now - RNG.nextInt(365) * 86400000L);
            dp.setUnit(metricDefinitions.get(i % metricDefinitions.size()).getUnit());
            dp.addDimension("region", "region-" + (i % 5));
            dp.addDimension("service", "service-" + (i % 20));
            dataPoints.add(dp);
        }

        // Reports
        for (int i = 0; i < s(2000); i++) {
            DateRange dr = dateRanges.get(i % dateRanges.size());
            Report r = new Report(ID_GEN.nextId(), "Report-" + i, dr);
            r.setFormat(i % 3 == 0 ? "PDF" : i % 3 == 1 ? "CSV" : "HTML");
            r.setPublished(RNG.nextBoolean());
            if (!employees.isEmpty()) r.setAuthor(employees.get(RNG.nextInt(employees.size())));
            // Add data points
            int numDp = 10 + RNG.nextInt(90);
            for (int j = 0; j < numDp; j++) {
                r.addDataPoint(dataPoints.get((i * 100 + j) % dataPoints.size()));
            }
            reports.add(r);
        }

        // Dashboards
        for (int i = 0; i < s(1000); i++) {
            Dashboard d = new Dashboard(ID_GEN.nextId(), "Dashboard-" + i);
            d.setShared(RNG.nextBoolean());
            d.setLayout(RNG.nextBoolean() ? "GRID" : "FREEFORM");
            if (!employees.isEmpty()) d.setOwner(employees.get(RNG.nextInt(employees.size())));
            dashboards.add(d);
        }

        // Widgets (uses TreeMap)
        String[] widgetTypes = {"CHART","TABLE","GAUGE","MAP"};
        for (int i = 0; i < s(5000); i++) {
            Dashboard d = dashboards.get(i % dashboards.size());
            Widget w = new Widget(ID_GEN.nextId(), "Widget-" + i,
                widgetTypes[RNG.nextInt(widgetTypes.length)], d);
            w.setDataSource("source-" + (i % 20));
            w.setConfig("refresh_rate", String.valueOf(5 + RNG.nextInt(55)));
            w.setConfig("color_scheme", "scheme-" + (i % 5));
            w.setWidth(1 + RNG.nextInt(4));
            w.setHeight(1 + RNG.nextInt(3));
            d.addWidget(w);
            widgets.add(w);
        }

        // Trends (uses long[] and double[])
        String[] directions = {"UP","DOWN","STABLE"};
        for (int i = 0; i < s(5000); i++) {
            MetricDefinition md = metricDefinitions.get(i % metricDefinitions.size());
            int points = 30 + RNG.nextInt(335);
            Trend t = new Trend(ID_GEN.nextId(), md, points);
            for (int j = 0; j < points; j++) {
                t.setTimestamp(j, now - (points - j) * 86400000L);
                t.setValue(j, 100 + RNG.nextDouble() * 900 + (j * (RNG.nextDouble() - 0.5) * 10));
            }
            t.setDirection(directions[RNG.nextInt(directions.length)]);
            t.setChangePercent(-50 + RNG.nextDouble() * 100);
            trends.add(t);
        }

        // Anomalies
        for (int i = 0; i < s(5000); i++) {
            MetricDefinition md = metricDefinitions.get(i % metricDefinitions.size());
            double expected = 100 + RNG.nextDouble() * 900;
            double actual = expected * (0.5 + RNG.nextDouble() * 1.5);
            Anomaly a = new Anomaly(ID_GEN.nextId(), md, actual, expected);
            a.setSeverity(CoreEnums.Severity.values()[RNG.nextInt(CoreEnums.Severity.values().length)]);
            a.setResolved(RNG.nextFloat() > 0.4f);
            if (a.isResolved()) a.setNotes("Resolved: root cause was " + (i % 5));
            anomalies.add(a);
        }

        // ExportJobs
        String[] formats = {"CSV","PDF","XLSX"};
        for (int i = 0; i < s(2000); i++) {
            Report r = reports.get(i % reports.size());
            ExportJob ej = new ExportJob(ID_GEN.nextId(), r, formats[RNG.nextInt(formats.length)]);
            CoreEnums.Status[] ejStatuses = {CoreEnums.Status.PENDING, CoreEnums.Status.ACTIVE, CoreEnums.Status.ARCHIVED};
            ej.setStatus(ejStatuses[RNG.nextInt(ejStatuses.length)]);
            ej.setOutputPath("/exports/report-" + i + "." + ej.getFormat().toLowerCase());
            if (!employees.isEmpty()) ej.setRequestedBy(employees.get(RNG.nextInt(employees.size())));
            if (ej.getStatus() == CoreEnums.Status.ARCHIVED) {
                ej.setCompletedAt(System.currentTimeMillis() - RNG.nextInt(30) * 86400000L);
                ej.setFileSizeBytes(1024 + RNG.nextInt(10485760));
            }
            exportJobs.add(ej);
        }
    }

    // ======================== SECURITY ========================

    static void createSecurity() {
        // UserAccounts
        for (int i = 0; i < s(200000); i++) {
            UserAccount ua = new UserAccount(ID_GEN.nextId(), "user" + i);
            ua.setPasswordHash(randomBytes(64));
            ua.setSalt(randomBytes(16));
            ua.setTotpSecret(randomChars(32));
            if (i < employees.size()) ua.setEmployee(employees.get(i));
            ua.setLocked(RNG.nextFloat() > 0.95f);
            ua.setLastLogin(System.currentTimeMillis() - RNG.nextInt(30) * 86400000L);
            userAccounts.add(ua);
        }

        // Permissions
        String[] resources = {"users","projects","reports","budgets","servers","databases","files","configs","logs","admin"};
        String[] actions = {"READ","WRITE","DELETE","ADMIN"};
        for (int i = 0; i < s(10000); i++) {
            Permission p = new Permission(ID_GEN.nextId(),
                resources[RNG.nextInt(resources.length)],
                actions[RNG.nextInt(actions.length)],
                RNG.nextFloat() > 0.2f);
            String[] scopes = {"GLOBAL","ORG","TEAM","SELF"};
            p.setScope(scopes[RNG.nextInt(scopes.length)]);
            permissions.add(p);
        }

        // Add permissions to user accounts
        for (int i = 0; i < userAccounts.size(); i++) {
            int numPerms = 2 + RNG.nextInt(8);
            for (int j = 0; j < numPerms; j++) {
                userAccounts.get(i).addRole(permissions.get((i * 10 + j) % permissions.size()));
            }
        }

        // Sessions
        for (int i = 0; i < s(20000); i++) {
            UserAccount ua = userAccounts.get(RNG.nextInt(userAccounts.size()));
            Session s = new Session(ID_GEN.nextId(), ua, "tok_" + Long.toHexString(RNG.nextLong()));
            s.setIpAddress("192.168." + RNG.nextInt(256) + "." + RNG.nextInt(256));
            s.setUserAgent("Mozilla/5.0 (Agent " + (i % 50) + ")");
            s.setActive(RNG.nextFloat() > 0.6f);
            ua.addSession(s);
            sessions.add(s);
        }

        // AuditLogs (very high volume)
        String[] auditActions = {"LOGIN","LOGOUT","VIEW","EDIT","DELETE","CREATE","EXPORT","IMPORT"};
        for (int i = 0; i < s(1500000); i++) {
            UserAccount ua = userAccounts.get(RNG.nextInt(userAccounts.size()));
            AuditLog al = new AuditLog(ID_GEN.nextId(), ua,
                auditActions[RNG.nextInt(auditActions.length)],
                resources[RNG.nextInt(resources.length)]);
            al.setIpAddress("10." + RNG.nextInt(256) + "." + RNG.nextInt(256) + "." + RNG.nextInt(256));
            al.setSuccess(RNG.nextFloat() > 0.05f);
            al.setDetails("Details for audit log entry " + i);
            auditLogs.add(al);
        }

        // Credentials
        String[] credTypes = {"PASSWORD","API_KEY","SSH_KEY","OAUTH"};
        for (int i = 0; i < s(20000); i++) {
            UserAccount ua = userAccounts.get(i % userAccounts.size());
            Credential c = new Credential(ID_GEN.nextId(), ua, credTypes[RNG.nextInt(credTypes.length)]);
            c.setValue("cred_" + Long.toHexString(RNG.nextLong()));
            c.setRevoked(RNG.nextFloat() > 0.9f);
            credentials.add(c);
        }

        // SecurityPolicies (uses ConcurrentHashMap)
        String[] policyNames = {"Password Policy","Access Policy","Network Policy","Data Policy","Encryption Policy"};
        for (int i = 0; i < s(100); i++) {
            SecurityPolicy sp = new SecurityPolicy(ID_GEN.nextId(),
                policyNames[i % policyNames.length] + "-" + i);
            sp.setDescription("Security policy " + i + " governing access and data handling");
            sp.setEnforced(RNG.nextFloat() > 0.1f);
            sp.setMaxSessionDurationMinutes(60 + RNG.nextInt(420));
            sp.addRule("min_password_length", String.valueOf(8 + RNG.nextInt(8)));
            sp.addRule("require_mfa", String.valueOf(RNG.nextBoolean()));
            sp.addRule("max_failed_attempts", String.valueOf(3 + RNG.nextInt(7)));
            sp.addRule("session_timeout_minutes", String.valueOf(15 + RNG.nextInt(45)));
            sp.addRule("ip_whitelist_enabled", String.valueOf(RNG.nextBoolean()));
            securityPolicies.add(sp);
        }

        // ThreatEvents
        String[] threatTypes = {"BRUTE_FORCE","SQL_INJECTION","XSS","DDOS","PHISHING"};
        for (int i = 0; i < s(5000); i++) {
            ThreatEvent te = new ThreatEvent(ID_GEN.nextId(),
                "10." + RNG.nextInt(256) + "." + RNG.nextInt(256) + "." + RNG.nextInt(256),
                "target-service-" + (i % 20),
                threatTypes[RNG.nextInt(threatTypes.length)]);
            te.setSeverity(CoreEnums.Severity.values()[RNG.nextInt(CoreEnums.Severity.values().length)]);
            te.setMitigated(RNG.nextFloat() > 0.3f);
            if (te.isMitigated()) te.setResponse("Blocked source IP and notified security team");
            threatEvents.add(te);
        }
    }

    // ======================== CROSS-REFERENCES ========================

    static void wireCrossReferences() {
        System.out.println("Wiring cross-references...");

        // Add employees to department employee lists
        for (int i = 0; i < employees.size(); i++) {
            Department d = departments.get(i % departments.size());
            d.addEmployee(employees.get(i));
        }

        // Add employees to teams
        for (int i = 0; i < employees.size(); i++) {
            Team t = teams.get(i % teams.size());
            t.addMember(employees.get(i));
        }

        // Set team leads
        for (int i = 0; i < teams.size() && i < employees.size(); i++) {
            teams.get(i).setLead(employees.get(i));
        }

        // Add employees to org events
        for (int i = 0; i < orgEvents.size(); i++) {
            int numAttendees = 5 + RNG.nextInt(20);
            for (int j = 0; j < numAttendees && j < employees.size(); j++) {
                orgEvents.get(i).addAttendee(employees.get((i * 20 + j) % employees.size()));
            }
        }

        // Set committee chairpersons and members
        for (int i = 0; i < committees.size(); i++) {
            if (!employees.isEmpty()) {
                committees.get(i).setChairperson(employees.get(i % employees.size()));
                for (int j = 0; j < 5; j++) {
                    committees.get(i).addMember(employees.get((i * 5 + j) % employees.size()));
                }
            }
        }

        // Set division heads
        for (int i = 0; i < divisions.size() && i < employees.size(); i++) {
            divisions.get(i).setHead(employees.get(i));
        }

        // Link employees to accounts
        for (int i = 0; i < Math.min(employees.size(), accounts.size()); i++) {
            employees.get(i).addAccount(accounts.get(i));
        }

        // Link employees to projects
        for (int i = 0; i < employees.size(); i++) {
            int numProjects = 1 + RNG.nextInt(3);
            for (int j = 0; j < numProjects && !projects.isEmpty(); j++) {
                employees.get(i).addProject(projects.get((i + j) % projects.size()));
            }
        }

        // Set team projects
        for (int i = 0; i < teams.size() && !projects.isEmpty(); i++) {
            teams.get(i).addProject(projects.get(i % projects.size()));
        }

        // Set budgets on departments
        for (int i = 0; i < Math.min(departments.size(), budgets.size()); i++) {
            departments.get(i).setBudget(budgets.get(i));
        }

        // Set org employee counts
        for (Organization org : organizations) {
            org.setEmployeeCount(org.getDepartments().size() * 10);
        }

        System.out.println("Cross-references wired successfully.");
    }

    // ======================== STATS ========================

    static void printStats() {
        Runtime rt = Runtime.getRuntime();
        long usedMB = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        long totalMB = rt.totalMemory() / (1024 * 1024);
        System.out.println("\n=== Heap Statistics ===");
        System.out.println("Used memory:  " + usedMB + " MB");
        System.out.println("Total memory: " + totalMB + " MB");
        System.out.println("Max memory:   " + (rt.maxMemory() / (1024 * 1024)) + " MB");
        System.out.println("\n=== Instance Counts ===");

        System.out.println("Core:       configs=" + configurations.size() + " addresses=" + addresses.size() + " dateRanges=" + dateRanges.size());
        System.out.println("Org:        orgs=" + organizations.size() + " depts=" + departments.size() + " teams=" + teams.size() +
            " divisions=" + divisions.size() + " offices=" + offices.size() + " costCenters=" + costCenters.size());
        System.out.println("HR:         employees=" + employees.size() + " contractors=" + contractors.size() + " recruiters=" + recruiters.size() +
            " payrolls=" + payrolls.size() + " benefits=" + benefitsList.size() + " reviews=" + reviews.size() + " attendance=" + attendances.size());
        System.out.println("Finance:    accounts=" + accounts.size() + " transactions=" + transactions.size() + " budgets=" + budgets.size() +
            " invoices=" + invoices.size() + " ledgerEntries=" + ledgerEntries.size() + " taxRecords=" + taxRecords.size());
        System.out.println("Projects:   projects=" + projects.size() + " tasks=" + tasks.size() + " sprints=" + sprints.size() +
            " milestones=" + milestones.size() + " releases=" + releases.size() + " riskItems=" + riskItems.size());
        System.out.println("Infra:      servers=" + servers.size() + " networks=" + networks.size() + " deployments=" + deployments.size() +
            " monitors=" + monitors.size() + " alerts=" + alerts.size() + " databases=" + databases.size());
        System.out.println("Inventory:  products=" + products.size() + " warehouses=" + warehouses.size() + " stockRecords=" + stockRecords.size() +
            " suppliers=" + suppliers.size() + " purchaseOrders=" + purchaseOrders.size() + " shipments=" + shipments.size());
        System.out.println("Comms:      channels=" + channels.size() + " messages=" + messages.size() + " attachments=" + attachments.size() +
            " notifications=" + notifications.size() + " chatRooms=" + chatRooms.size());
        System.out.println("Analytics:  dataPoints=" + dataPoints.size() + " reports=" + reports.size() + " dashboards=" + dashboards.size() +
            " widgets=" + widgets.size() + " trends=" + trends.size() + " anomalies=" + anomalies.size());
        System.out.println("Security:   userAccounts=" + userAccounts.size() + " permissions=" + permissions.size() + " auditLogs=" + auditLogs.size() +
            " sessions=" + sessions.size() + " credentials=" + credentials.size() + " threatEvents=" + threatEvents.size());

        long totalInstances = configurations.size() + addresses.size() + dateRanges.size()
            + organizations.size() + departments.size() + teams.size() + divisions.size()
            + orgCharts.size() + offices.size() + orgPolicies.size() + costCenters.size()
            + orgEvents.size() + subsidiaries.size() + committees.size() + orgMetricsList.size()
            + employees.size() + contractors.size() + recruiters.size() + roles.size()
            + payrolls.size() + benefitsList.size() + reviews.size() + attendances.size()
            + trainingRecords.size() + leaveRequests.size() + directories.size()
            + accounts.size() + transactions.size() + budgets.size() + invoices.size()
            + lineItems.size() + ledgers.size() + ledgerEntries.size() + taxRecords.size()
            + paymentMethods.size() + financialReports.size() + currencies.size()
            + projects.size() + tasks.size() + sprints.size() + milestones.size()
            + releases.size() + riskRegisters.size() + riskItems.size() + backlogs.size()
            + retrospectives.size() + dependencies.size() + projectMetricsList.size()
            + servers.size() + networks.size() + deployments.size() + monitors.size()
            + alerts.size() + loadBalancers.size() + databases.size() + sslCertificates.size()
            + cloudRegions.size() + firewalls.size()
            + products.size() + warehouses.size() + stockRecords.size() + suppliers.size()
            + purchaseOrders.size() + purchaseOrderLines.size() + shipments.size()
            + categories.size() + inventoryAudits.size() + returnRecords.size()
            + channels.size() + messages.size() + attachments.size() + notifications.size()
            + emailTemplates.size() + announcements.size() + chatRooms.size()
            + surveyResponses.size() + contactLists.size()
            + reports.size() + dataPoints.size() + dashboards.size() + widgets.size()
            + metricDefinitions.size() + trends.size() + anomalies.size() + exportJobs.size()
            + userAccounts.size() + permissions.size() + auditLogs.size() + sessions.size()
            + credentials.size() + securityPolicies.size() + threatEvents.size();

        System.out.println("\nTotal custom instances: " + totalInstances);
    }
}
