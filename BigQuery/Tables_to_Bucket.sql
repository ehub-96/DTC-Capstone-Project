# Execute Separately

EXPORT DATA
  OPTIONS(
    uri='gs://dtc_project_bucket/*DTCP_french_clustered.csv',
    format='CSV',
    overwrite=true,
    field_delimiter=',',
    header=true
  )
  AS
  SELECT *
  FROM `DTCP_dataset.Clustered_County`

EXPORT DATA
  OPTIONS(
    uri='gs://dtc_project_bucket/*DTCP_French_Radioactive_Waste_Clean.csv', 
    format='CSV',
    overwrite=true,
    field_delimiter=',',
    header=true
  )
  AS
  SELECT *
  FROM `DTCP_dataset.French_Radioactive_Waste`

  EXPORT DATA
  OPTIONS(
    uri='gs://dtc_project_bucket/*DTCP_Radionuclides.csv', 
    format='CSV',
    overwrite=true,
    field_delimiter=',',
    header=true
  )
  AS
  SELECT *
  FROM `DTCP_dataset.Radionuclides`
