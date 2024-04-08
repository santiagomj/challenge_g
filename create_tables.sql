CREATE DATABASE chall_g;

CREATE TABLE chall_g.departments (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    department_name VARCHAR(255) NOT NULL
);

CREATE TABLE chall_g.jobs (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL
);

CREATE TABLE chall_g.employees (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    datetime VARCHAR(255) NOT NULL,
    department_id INTEGER,
    job_id INTEGER
);