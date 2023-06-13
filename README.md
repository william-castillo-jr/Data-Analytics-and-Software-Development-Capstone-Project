# Data Analytics and Software Development Capstone Project

## Authors
William Castillo, Addison Farley, RJ Trancherd, Mason Sain, Noah Lanctot

## Overview

This project focuses on automating the collection, processing, and storage of solar generation forecast data. The aim is to provide a consistent and reliable data source for further analysis. Through teamwork and collaboration, we successfully automated the entire data pipeline, ensuring efficiency, reliability, and data integrity.

## Running the Program
To run this file, you will need to have a Google Cloud account with BigQuery enabled. Follow these steps:

1. Create a Google Cloud account if you don't have one already: [Google Cloud Console](https://console.cloud.google.com/).
2. Enable BigQuery in your Google Cloud account.
3. Create and download a service account key JSON file by following the instructions [here](https://console.cloud.google.com/iam-admin/serviceaccounts/). This JSON file will be used to authenticate your program with Google Cloud services.
4. Add the downloaded service account key JSON file to the program's directory.
5. Open the `solv4solvx.py` file and update the following fields:
    - `filepath`: Set the filepath to the location of your service account key JSON file.
    - Commented fields: Review and update any other required fields in the script, such as database credentials or table names, as per your desired SQL database configuration.
6. Once you have completed these steps, the script will automatically pull 2 days' worth of energy data for the PNW area and upload it to your desired SQL database in BigQuery every 24 hours.

Please make sure to secure your service account key JSON file and avoid sharing it publicly. It contains sensitive information that grants access to your Google Cloud resources.

If you have any questions or run into issues during setup, please refer to the documentation provided by Google Cloud or reach out for assistance.

![System Diagram](https://github.com/william-castillo-jr/Data-Analytics-and-Software-Development-Capstone-Project/assets/135763064/b8b8a7c1-3c6f-4861-b97c-de0afe06da75)
