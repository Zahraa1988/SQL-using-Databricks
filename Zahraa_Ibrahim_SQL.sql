-- Databricks notebook source
-- MAGIC %md ### creating a variable and csv files as reusable code for it later in PySpark SQL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC my_csv_file = "clinicaltrial_2021.csv"
-- MAGIC my_File = "dbfs:/FileStore/tables/" + my_csv_file
-- MAGIC pharma_File_csv = "pharma.csv"
-- MAGIC pharma_File = "dbfs:/FileStore/tables/" + pharma_File_csv

-- COMMAND ----------

-- MAGIC %md ####Created a dataframe for two.csv files (clinicaltrial_2021 and pharma_df).

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## clinicaltrial_2021 dataframe
-- MAGIC dataframe = spark.read.options(header=True).option("delimiter" , "|").csv(my_File)
-- MAGIC display(dataframe)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # pharma dataframe
-- MAGIC pharma_df = spark.read.options(header=True).option("delimiter" , ",").csv(pharma_File)
-- MAGIC pharma_df.limit(3).display()

-- COMMAND ----------

-- MAGIC %md ####Creating Temprory view

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataframe.createOrReplaceTempView("SQL2021")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma_df.createOrReplaceTempView("PHARMA2021")

-- COMMAND ----------

-- MAGIC %md ##### Display the views

-- COMMAND ----------

SELECT * FROM SQL2021 LIMIT 10

-- COMMAND ----------

SELECT * FROM PHARMA2021 LIMIT 10

-- COMMAND ----------

-- MAGIC %md ###changed the two temporary views to permanent views.

-- COMMAND ----------

CREATE OR REPLACE TABLE default.clinicaltrial_2021 as select * from SQL2021

-- COMMAND ----------

CREATE OR REPLACE TABLE default.PHARMA2021 as select * from PHARMA2021

-- COMMAND ----------

-- MAGIC %md #### created temporary views

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW split_conditions AS SELECT Conditions, split(Conditions, ",") AS Conditions_Array
FROM clinicaltrial_2021

-- COMMAND ----------

SELECT * FROM split_conditions

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW number_studies AS SELECT Status, split(Completion, " ")[0] AS Months,split(Completion, " ")[1] AS Years
FROM clinicaltrial_2021

-- COMMAND ----------

SELECT * FROM number_studies

-- COMMAND ----------

-- MAGIC %md #### display the views

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md ### count the number of studies in the dataset

-- COMMAND ----------

SELECT DISTINCT COUNT (*) AS Number_Studies FROM clinicaltrial_2021

-- COMMAND ----------

-- MAGIC %md ### list all the types of studies in the dataset along with the frequencies.

-- COMMAND ----------

SELECT Type AS Type_of_Studies, count(Type) AS Frequencies
FROM clinicaltrial_2021
GROUP BY Type
ORDER BY Frequencies DESC

-- COMMAND ----------

-- MAGIC %md ##### The top 5 conditions with their frequencies.

-- COMMAND ----------

-- MAGIC %md ###### Create another temporary view from the temporary view that we have created in data preparation.

-- COMMAND ----------

drop view if exists Conditions_Frequency;
CREATE OR REPLACE TEMP VIEW Conditions_Frequency AS SELECT Conditions, explode(Conditions_Array) AS conditions_frequencies
From split_conditions

-- COMMAND ----------

SELECT * FROM Conditions_Frequency

-- COMMAND ----------

-- MAGIC %md #### working with Pyspark SQL

-- COMMAND ----------

SELECT conditions_frequencies,count(*) as count_conditions_frequencies
FROM Conditions_Frequency
GROUP BY conditions_frequencies
ORDER BY count_conditions_frequencies DESC LIMIT (5)

-- COMMAND ----------

-- MAGIC %md ###### The 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW Join_Tables AS SELECT D.Sponsor, P.Parent_Company
FROM clinicaltrial_2021 D LEFT JOIN PHARMA2021 P
ON D.Sponsor = P.Parent_Company

-- COMMAND ----------

SELECT * 
FROM Join_Tables
LIMIT 3

-- COMMAND ----------

SELECT Sponsor, count(*) AS common_sponsor
FROM Join_Tables
WHERE Parent_Company IS NULL
GROUP BY Sponsor
ORDER BY common_sponsor DESC LIMIT 10


-- COMMAND ----------

-- MAGIC %md #### Q5: Find the number of studies in each months in 2021

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW total_studies AS SELECT Months,Status, count(Months) AS count_studies
FROM number_studies
WHERE Years = "2021" AND Status = "Completed"
GROUP BY Status,Months

-- COMMAND ----------

SELECT (*) FROM total_studies

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW completed_studies AS SELECT count_studies,Months,
CASE
WHEN Months ="Jan" THEN 1
WHEN Months ="Feb" THEN 2
WHEN Months ="Mar" THEN 3
WHEN Months ="Apr" THEN 4
WHEN Months ="May" THEN 5
WHEN Months ="Jun" THEN 6
WHEN Months ="Jul" THEN 7
WHEN Months ="Aug" THEN 8
WHEN Months ="Sep" THEN 9
WHEN Months ="Oct" THEN 10
WHEN Months ="Nov" THEN 11
WHEN Months ="Dec" THEN 12
END AS Months_Order
FROM total_studies


-- COMMAND ----------

SELECT Months,count_studies
FROM completed_studies
SORT BY Months_Order ASC LIMIT 12

-- COMMAND ----------

CREATE OR REPLACE TABLE default.Months_count as select * from completed_studies

-- COMMAND ----------

SELECT * FROM Months_count

-- COMMAND ----------

-- MAGIC %md #### Check the permanent view.

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md ####Further analysis

-- COMMAND ----------

-- MAGIC %md ###### Find the most common Primary_Offense that are pharmaceutical companies, along with the number of clinical trials they have sponsored.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW Further_Analysis AS SELECT C.Sponsor, P.Parent_Company, P.Primary_Offense, P.Penalty_Year
FROM clinicaltrial_2021 C Inner JOIN PHARMA2021 P
ON C.Sponsor = P.Parent_Company

-- COMMAND ----------

SELECT * FROM Further_Analysis LIMIT 10

-- COMMAND ----------

SELECT Sponsor, Parent_Company,Primary_Offense, count(*) AS Sponsor_count 
FROM Further_Analysis
WHERE Penalty_Year = "2021" AND Primary_Offense IS NOT NULL
GROUP BY Sponsor,Parent_Company, Primary_Offense
ORDER BY Sponsor_count DESC LIMIT 10


-- COMMAND ----------

CREATE OR REPLACE TABLE default.primary_offense as select * from Further_Analysis

-- COMMAND ----------

SHOW TABLES
