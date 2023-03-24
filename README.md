# Data Engineering Zoomcamp - Capstone Proyect - Erik Hub



## Index

- [Problem Description](#problem-description)
- [Analysis](#analysis)
- [Cloud](#cloud)
- [Data Ingestion](#data-ingestion)
- [Data Warehouse](#data-warehouse)
- [Transformation](#transformation)
- <a href="https://docs.google.com/document/d/1IQIYw_qAf6k3L5TfHRjHJ2gpLz7aqjbHPem2s41xLQw/edit?usp=sharing" target="_blank">Document</a>
- <a href="https://public.tableau.com/app/profile/erik.hub5895/viz/DTCDEDashboard/Story1" target="_blank">Dashboard</a>
- [Contact](#contact)




### Problem Description

This dataset contains information on the location, type, and description of various radioactive waste materials. However, there is a significant issue of uncertainty regarding the type and description of some entries in the dataset. This lack of information can impede the accurate assessment of potential health and environmental risks associated with these materials. Therefore, there is a need to address this uncertainty and ensure that the dataset provides complete and reliable information on radioactive waste materials. 

### Analysis

The analysis of the radioactive waste dataset revealed several key findings. First, we identified the most common isotopes present in the dataset, which included Cobalt, Caesium and Tritium . This information is crucial for assessing the proper storage and potential health/environmental risks associated with these materials.
Additionally, the facilities with the highest volume of radioactive waste were identified.By understanding which facilities possess the most radioactive waste and its origin, we can develop targeted interventions to reduce the amount of waste produced and minimize potential risks or auditions to ensure the proper storage or disposal of such materials.
Furthermore, we found that the Essonne department has the most residues in the dataset. This information is important for local authorities to take appropriate measures to ensure the safe handling and disposal of radioactive waste in the area.
Finally, our analysis revealed a significant level of uncertainty in the dataset, particularly with regard to missing information on the type and description of some entries. To address this issue, we recommend implementing measures to improve the accuracy and completeness of the dataset. This may include conducting additional surveys, establishing standardized reporting requirements, and improving communication between facilities and regulatory agencies to ensure that all relevant information is captured or by simply updating the dataset when the next iteration of it is published. By addressing these uncertainties, we can ensure that the dataset provides reliable and comprehensive information on these waste materials.


### Cloud

Google cloud services are used, for access to the project please read the “Google Cloud Project” file in the “README” folder.


### Data Ingestion

Batch/Workflow is used, several steps with Luigi for download, transformation and upload, the code used is in the “Pipeline” folder, Terraform is used to configure and upload to the Google Cloud Services.

### Data Warehouse

Tables need to be created manually. Data is manipulated with BigQuery, both clustered and partitioned for better understanding the “Counties” and “Isotopes” data; code used is both saved in Google BigQuery and the “BigQuery” folder.

### Transformation

Just simple SQL transformation, code used is both saved in Google BigQuery and the “BigQuery” folder, Jupiter was used for most of the data exploration, the notebook can be found in the “Data Exploration” folder, and the data used and transformed can be found in the “Data” folder.

### Contact





- <a href="https://t.me/Ehub96" target="_blank">Telegram</a>
- <a href="https://www.linkedin.com/in/erik-hub-0346001b6/" target="_blank">Linkedin</a>
- <a href="https://twitter.com/ehub96?t=hQ_1F1a5bBjxX6k1RJCqQg&s=08" target="_blank">Twitter</a>
- <a href="https://t.me/Ehub96" target="_blank">Telegram</a>
- <a href="mailto:ehub96.dev@gmail.com"Email</a>
