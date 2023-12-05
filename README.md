# Motor Vehicle Collisions Analysis Project

## Project Background

The goal is to develop a data pipeline following the architecture below. We looked at Motor Vehicle Collisions data.

![image](https://github.com/Drummer05/Motor-Vehicle-Collisions---Crashes/assets/144565034/4acaa491-2801-4eb1-9853-aa4b34c7d469)

### Introduction
This project leverages the "Motor Vehicle Collisions - Crashes" dataset provided by the City of New York, which offers comprehensive insights into traffic-related incidents across New York City. The dataset includes detailed information on various aspects of motor vehicle collisions, such as dates and times of incidents, locations, types of vehicles involved, contributing factors, and the statistics on injuries and fatalities. It's available for public access [here](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95).

Updated daily, this dataset underscores New York City's commitment to road safety and data transparency. It serves as an essential tool for understanding traffic patterns, identifying high-risk zones, and developing strategies to improve road safety. The objective of our project is to analyze this dataset to identify trends, gain insights, and propose potential solutions to mitigate traffic-related incidents in New York City.

To access the data dictionary:[Data Dictionary](https://docs.google.com/spreadsheets/d/1wpSyFV4l6buYbJLw9VdQ2wDpVaEHRzUmPSIdX4RFfrE/edit#gid=0)

### Project Overview
The essence of this project is the construction of a data warehouse, embodying a thorough approach to data management from acquisition to visualization. It encompasses the entire spectrum of data processes: collection, storage, transformation, and presentation. This initiative demands a combination of technical skills in data handling and a deep understanding of data architecture and visualization techniques.

For additional details about the project requirements, please view the [Project Requirement Document](https://docs.google.com/document/d/1_kOnDBnnz1eypVWkyvQCl2P9orBKbbJV1yRB0RdKzTI/edit).

### Stakeholder Requirements
The project's main stakeholder, Professor Jefferson Bien-Aime, has outlined specific requirements for a dashboard to glean insights into vehicle and motor crashes in New York City. The dashboard aims to answer the following questions:

- Accident Count by Borough
- Accident Count by Year
- Top Ten Causes of Accidents
- Top Ten Types of Vehicles/Motors Involved in Crashes
- Accident Count by Season
- Accident Count by Time of Day
- Accident Count by Month
- Total Number of Injuries per Accident
- Total Number of Fatalities
- Total Number of Accidents

### Project Structure

1. **Gather Requirements:** Gather the neccesary requirements to solve the problem.
2. **Understand the Data:** Get familiarize with the datasets. Understand the columns.
3. **Dimensional Modeling**: Create facts and dimensional tables
4. **Data Scraping and Cloud Upload:** Write a Python script to scrape data from the source and save it in google cloud storage.
5. **Data Pipeline/ETL:** Transport the data to anaconda python, where the data will be transformed.
6. **Data Loading:** Create the facs and Dimensions table on Google Bigquery, then Load the cleaned data into the data warehouse, insert the data into the tables and finally connect Google Bigquery to Tableau for data visualizaiton.

### Files in the Repository
- **Data Extraction**
  - [Data Extraction NYC Collision Data Script](./Data%20Extraction%20NYC_Collision_data%20(1).py): Python script for extracting the collision data from the NYC dataset.
- **Data Transformation**
  - [Data Transformation NYC Collision Data Script](./Data%20Transformation%20NYC_Collision_data%20(1).py): Python script for transforming the loaded data for analytical purposes.
- **Data Loading**
  - - [Data Loading NYC Collision Data Script](./Data%20Loading%20NYC_Collision_data.py): Python script for loading the extracted data into a suitable format for analysis.
- **Dimensional Modeling**
  - [Dimensional Modeling.png](./Dimensional%20Modeling.png): Image file illustrating the dimensional modeling used in this project.
  - Data Transformation NYC_Collision_data (1).py

The creation of a Tableau dashboard will facilitate the visualization and analysis of these metrics.
