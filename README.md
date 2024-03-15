# OMOPDataQualityDashboard

## Introduction

To start, let's define what a data quality check is. It can be characterized as an aggregated summary statistic computed from the dataset, against which a decision threshold can be applied to determine if the statistic meets the expected standard.
The quality assessments within our system adhere to the Kahn Framework , which provides a structured approach to evaluating data quality through a system of categories and contexts. To familiarize yourself with this framework, please refer to the provided [link].(https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5051581/)

![image](https://github.com/Aasimzada/OMOP-Data-Quality-Dashboard/assets/163043181/2cb9a75c-1bb2-4584-9e50-74103593fa34)

For better comprehension, you can also refer to this [documentation](https://www.ohdsi.org/wp-content/uploads/2019/09/2-Plenary-1-OMOP-DQ-Clair-Andrew.pdf), which provides examples to aid understanding.

## Overview

Our Data Quality Dashboard employs a systematic approach to conducting data quality assessments. Instead of creating numerous individual checks, we utilize "data quality check types." These check types offer broader, parameterized evaluations of data quality, accommodating various aspects of OMOP tables, fields, and concepts, while encapsulating a singular idea of data quality. 

For instance, let's consider a different check type formulated as follows:

Determining the number and percentage of records with a value in the "LAB_RESULT" field of the "LAB_OBSERVATION" table that falls below a certain threshold.

This exemplifies an atemporal plausibility verification check, aiming to flag unusually low values in laboratory results based on internal knowledge. By applying this check type, specific values for the field "LAB_RESULT," table "LAB_OBSERVATION," and the threshold can be substituted to create tailored data quality assessments. For instance, when applied to "LAB_RESULT.HDL_CHOLESTEROL," it might manifest as:

Identifying the number and percentage of records with a value in the HDL cholesterol field of the laboratory observations table that is lower than 10 mg/dL.

Similarly, due to its parameterized nature, a comparable approach can be applied to another scenario such as "LAB_RESULT.BLOOD_GLUCOSE":

Identifying the number and percentage of records with a value in the blood glucose field of the laboratory observations table that is lower than 70 mg/dL.

This illustrates how the same data quality check type can be adapted and applied to different fields within the dataset, allowing for comprehensive assessments of data quality across various domains.

Another example,For example, let's consider ensuring the plausibility of recorded blood pressure values. We can create a check to identify potentially erroneous readings:

Identifying the number and percentage of records with a value in the SYSTOLIC_BLOOD_PRESSURE field of the MEASUREMENT table less than 50 mmHg or greater than 250 mmHg.

Similarly, we can extend this check to diastolic blood pressure:

Identifying the number and percentage of records with a value in the DIASTOLIC_BLOOD_PRESSURE field of the MEASUREMENT table less than 30 mmHg or greater than 150 mmHg.

These checks are based on clinical knowledge of typical blood pressure ranges and can help flag potentially erroneous or implausible readings in the dataset. By applying such checks, we aim to ensure the quality and reliability of the recorded blood pressure data in the OMOP CDM.

This version of our tool includes 24 distinct check types, categorized into Kahn contexts and categories. Each data quality check type is further classified as a table check, field check, or concept-level check. Table-level checks evaluate tables at a higher level, ensuring required tables are present or verifying the existence of records for individuals in the PERSON table within event tables. Field-level checks pertain to specific fields in a table and constitute the majority of Version 1 checks. These include evaluations of primary key relationships and investigations into whether concepts in a field adhere to specified domains. Concept-level checks relate to individual concepts and include evaluations such as identifying gender-specific concepts in individuals of the incorrect gender and assessing plausible values for measurement-unit pairs. Detailed descriptions and definitions of each check type are available in the provided link.

Following the systematic application of the 24 check types to an OMOP CDM version, approximately 4,000 individual data quality checks are resolved, executed against the database, and evaluated based on predefined thresholds.
