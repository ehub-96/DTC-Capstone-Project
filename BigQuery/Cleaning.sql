SELECT column_name, data_type
FROM `dtc-capstone-project.DTCP_dataset`.INFORMATION_SCHEMA.COLUMNS

ALTER TABLE `dtc-capstone-project.DTCP_dataset.French_Radioactive_Waste`
DROP COLUMN `Waste_Sub_Type`;

ALTER TABLE `dtc-capstone-project.DTCP_dataset.French_Radioactive_Waste`
DROP COLUMN `Category`;

ALTER TABLE `dtc-capstone-project.DTCP_dataset.French_Radioactive_Waste`
DROP COLUMN `Increase`;

CREATE TABLE `dtc-capstone-project.DTCP_dataset.Radionuclides`
AS
SELECT 
  *,
  SPLIT(Radionuclides, ',')[SAFE_OFFSET(0)] AS Radionuclide_1,
  SPLIT(Radionuclides, ',')[SAFE_OFFSET(1)] AS Radionuclide_2,
  SPLIT(Radionuclides, ',')[SAFE_OFFSET(2)] AS Radionuclide_3,
  SPLIT(Radionuclides, ',')[SAFE_OFFSET(3)] AS Radionuclide_4,
  SPLIT(Radionuclides, ',')[SAFE_OFFSET(4)] AS Radionuclide_5,
  SPLIT(Radionuclides, ',')[SAFE_OFFSET(5)] AS Radionuclide_6,
FROM 
  `DTCP_dataset.French_Radioactive_Waste`

# EXECUTE SPERATELY

CREATE TABLE `dtc-capstone-project.DTCP_dataset.Clustered_County`
CLUSTER BY County
AS
SELECT *
FROM `dtc-capstone-project.DTCP_dataset.French_Radioactive_Waste`;