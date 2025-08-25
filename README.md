# Big Data ETL Pipeline for NYC Job Applications

## Project Overview
This project showcases a comprehensive Big Data ETL (Extract, Transform, Load) pipeline that processes job application filings from the NYC Department of Buildings (DOB). The system is built with modern data engineering practices, including an AI-powered data enrichment step using a Large Language Model (LLM).

**Key highlights of this project include:**
* **Modular ETL Architecture:** The ETL logic is separated into distinct extract, transform, and load stages to ensure maintainability and clarity.
* **AI Integration:** A Large Language Model (Groq's LLM API) was used for intelligent extraction of job roles from unstructured text descriptions.
* **Automated Testing:** The pipeline includes a suite of comprehensive unit tests for each transformation stage.
* **CI/CD Automation:** A Jenkins pipeline was set up to automatically run the correct pipeline based on the Git branch.

## About Dataset:
The dataset comprises all job applications submitted through the Borough Offices, eFiling, or the HUB with a "Latest Action Date" from January 1, 2000, onwards. It does not include applications submitted via DOB NOW; a separate dataset exists for those. The dataset contains approximately 2.71 million records with 96 columns, providing detailed information for each job application.

**Dataset Link:** https://data.cityofnewyork.us/Housing-Development/DOB-Job-Application-Filings/ic3t-wcy2/about_data

## Technologies Used:
This project leveraged a range of powerful tools and frameworks:
* **`Python` 3.13.5:** The core programming language for the project.
* **`PySpark` & `Apache Spark` 4.0.0:** Used as the big data and distributed data processing frameworks.
* **`Groq` API:** The LLM service utilized for extracting job roles.
* **`Jenkins`:** For CI/CD automation.
* **`Pytest`:** The unit testing framework.
* **`Git` & `GitHub`:** For version control and collaborative development with a branch-based strategy

## Branch-Based Development Strategy:
We adopted a branch-based development strategy using GitHub, with each new feature being developed, tested, and reviewed in its own branch before being merged into the main codebase. This approach allowed for incremental and parallel development of the pipeline's features.

<img width="800" height="800" alt="Screenshot 2025-08-22 175704" src="https://github.com/user-attachments/assets/9c6fcf29-51ba-432d-ba85-0e7700ff7da8" />

## Feature Branch Breakdown:
**1. feature1-data-extraction-and-table-creation:** This branch focused on the initial data ingestion, including reading raw parquet files, standardizing column names, and creating six core tables: `Job_Applications`, `Properties`, `Applicants`, `Owners`, `Job_status`, and `Boroughs`.

<img width="800" height="850" alt="Screenshot 2025-08-22 191906" src="https://github.com/user-attachments/assets/fe0c5019-965d-422c-9d74-a7d5ab2dde07" />
<br>
<br>
<br>

**2. feature2-3-tables-transformation:** The focus here was on improving data quality by performing cleaning, renaming columns, converting data types, and handling null values for the `Job_Applications`, `Properties`, and `Applicants tables`.

<img width="800" height="850" alt="Screenshot 2025-08-25 170357" src="https://github.com/user-attachments/assets/c0772b5e-6667-46e8-87a2-fa5f7f55d54e" />
<br>
<br>
<br>

**3. feature3-another-3-tables-transformation:** This branch extended the data transformations to the `Owners`, `Job_status`, and `Boroughs` datasets, including data cleanup and code mapping.

<img width="800" height="850" alt="Screenshot 2025-08-25 170556" src="https://github.com/user-attachments/assets/d10139b4-c01b-4031-babc-39c224bef32c" />
<br>
<br>
<br>

**4. feature4-some-few-transformation:** This branch was dedicated to specialized transformations, such as mapping job status codes to descriptive names and formatting owner names.

<img width="800" height="850" alt="Screenshot 2025-08-25 171513" src="https://github.com/user-attachments/assets/8f490f1e-a29c-4312-b46b-838a5abcbcbf" />
<br>
<br>
<br>

**5. feature5-LLM-usage:** The final feature branch, this integrated the Groq API to extract and enrich the data with job roles from unstructured text descriptions.

<img width="800" height="850" alt="Screenshot 2025-08-22 211905" src="https://github.com/user-attachments/assets/a43446e2-e59e-44de-b91c-3ecda76b6b79" />

## AI/ML Integration:
`Job_Status_Descrp` column. This step was crucial for standardizing the free-text job descriptions into clean, structured role labels, significantly improving the data's usability for downstream analysis.

The workflow for this was:
1. Read the raw `Job_Status_Descrp` column.
2. Send each description to the Groq LLM API.
3. Extract and clean the returned job role.
4. Create a new `Role` column with the standardized role labels.
5. Join the enriched data back with the original PySpark DataFrame

Sample output:

<img width="700" height="300" alt="Screenshot 2025-08-25 163153" src="https://github.com/user-attachments/assets/aaf0f8c1-17c8-4030-b03f-edeb6bdeade8" />

## Continuous Integration and Testing:
The project's reliability is ensured through a robust testing and CI/CD strategy.

**Testing Strategy:** We used Pytest with PySpark to create comprehensive unit tests for the transformation scripts. The tests cover everything from schema validation and column renaming to type conversions and null handling, including edge cases.

**CI/CD Pipeline with Jenkins:** The Jenkins pipeline is configured for branch-based automation. When a new commit is pushed, Jenkins automatically detects the active branch and runs the appropriate tests and pipeline for that branch. This includes stages for checking out the code, setting up the environment, running tests, and executing the ETL pipeline.

Example workflow:

<img width="800" height="850" alt="Screenshot 2025-08-22 231558" src="https://github.com/user-attachments/assets/bccd8f7e-197a-4308-9fc3-376f64e5445a" />

## Final ETL Processed Tables:

<img width="350" height="300" alt="Screenshot 2025-08-25 163919" src="https://github.com/user-attachments/assets/bd228179-630e-46d1-a000-c450ccee97a0" />







