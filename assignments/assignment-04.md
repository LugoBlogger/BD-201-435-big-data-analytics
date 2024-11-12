# Assignment 04

Full name (Student ID Number)

You can answer in English or Bahasa Indonesia.

## Problem 1

Given the following table

```bash
+-----------------+
|info             |
+-----------------+
|{Ended, 89, true}|
+-----------------+
```

with its schema
```bash
root
 |-- info: struct (nullable = false)
 |    |-- status: string (nullable = true)
 |    |-- weight: long (nullable = true)
 |    |-- has_watched: boolean (nullable = false)
```

Turn the table into the following form
```bash
+----------------------------------------------------+
|info                                                | 
+----------------------------------------------------+
|{status -> Ended, weight -> 89, has_watched -> true}|
+----------------------------------------------------+
```

**Hint**: First you need to create the table first, and then perform
several transformation to the table

## Problem 2  
(need revision, it might be too difficult)   
Construct the following table using Backblaze hard drive
datasets for a year 2019

<img src="../img-resources/07-backblaze-report-Q3-2019-Drive-Stats-table-V2.png">
