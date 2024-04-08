-- Requirement 1
SELECT
    d.department_name AS department,
    j.job_name AS job,
    COUNT(CASE WHEN QUARTER(e.datetime) = 1 THEN 1 END) AS Q1,
    COUNT(CASE WHEN QUARTER(e.datetime) = 2 THEN 1 END) AS Q2,
    COUNT(CASE WHEN QUARTER(e.datetime) = 3 THEN 1 END) AS Q3,
    COUNT(CASE WHEN QUARTER(e.datetime) = 4 THEN 1 END) AS Q4
FROM employees e
INNER JOIN departments d ON e.department_id = d.id
INNER JOIN jobs j ON e.job_id = j.id
WHERE YEAR(e.datetime) = 2021
GROUP BY d.department_name, j.job_name
ORDER BY d.department_name, j.job_name;


-- Requirement 2
SELECT
    d.id,
    d.department_name AS department,
    COUNT(*) AS hired
FROM employees e
INNER JOIN departments d ON e.department_id = d.id
GROUP BY d.id
HAVING COUNT(*) > (
    SELECT AVG(hired_count)
    FROM (
        SELECT COUNT(*) AS hired_count
        FROM employees
        WHERE YEAR(datetime) = 2021
        GROUP BY department_id
    ) AS avg_hired
)
ORDER BY hired DESC;