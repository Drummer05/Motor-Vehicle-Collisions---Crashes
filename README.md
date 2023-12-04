# Motor Vehicle Collisions Analysis Project

## Project Background

### Introduction
This project leverages the "Motor Vehicle Collisions - Crashes" dataset provided by the City of New York, which offers comprehensive insights into traffic-related incidents across New York City. The dataset includes detailed information on various aspects of motor vehicle collisions, such as dates and times of incidents, locations, types of vehicles involved, contributing factors, and the statistics on injuries and fatalities. It's available for public access [here](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95).

To access the data dictionary: 

Updated daily, this dataset underscores New York City's commitment to road safety and data transparency. It serves as an essential tool for understanding traffic patterns, identifying high-risk zones, and developing strategies to improve road safety. The objective of our project is to analyze this dataset to identify trends, gain insights, and propose potential solutions to mitigate traffic-related incidents in New York City.

### Project Overview
The essence of this project is the construction of a data warehouse, embodying a thorough approach to data management from acquisition to visualization. It encompasses the entire spectrum of data processes: collection, storage, transformation, and presentation. This initiative demands a combination of technical skills in data handling and a deep understanding of data architecture and visualization techniques.

### Files in the Repository
- **Data Extraction**
  - [NYC_Collision_data (1).py](./NYC_Collision_data%20(1).py): Python script for extracting the collision data from the NYC dataset.
- **Data Loading**
  - [NYC_Collision_data.py](./NYC_Collision_data.py): Python script for loading the extracted data into a suitable format for analysis.
- **Data Transformation**
  - [NYC_Collision_data (1).py](./NYC_Collision_data%20(1).py): Python script for transforming the loaded data for analytical purposes.
- **Dimensional Modeling**
  - [Dimensional Modeling.png](./Dimensional%20Modeling.png): Image file illustrating the dimensional modeling used in this project.

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

The creation of a Tableau dashboard will facilitate the visualization and analysis of these metrics.
