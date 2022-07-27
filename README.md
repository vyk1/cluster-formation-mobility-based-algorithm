# Cluster generator algorithm
## Setup
0. Configure your workspace as presented in [iFogSim](https://github.com/Cloudslab/iFogSim)

## Cluster generation
1. Generate clusters running perfeval/GeoGen.java
> the input file is always presented at dataset/edgeResources-melbCBD.csv

## Execution
2. Point the generated file in src/org/fog/mobilitydata/DataParser.java
3. Run perfeval/MicroserviceApp_RandomMobility_Clustering.java
4. View output in dataset/output_data.csv