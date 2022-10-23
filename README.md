# Cluster generator algorithm
This repo presents the algorithm developed in the [paper](https://link.springer.com/chapter/10.1007/978-3-031-19945-5_3). 

## Setup
0. Configure your workspace as presented in [iFogSim](https://github.com/Cloudslab/iFogSim)

## Cluster generation
1. Generate clusters running perfeval/GeoGen.java
> the input file is always presented at dataset/edgeResources-melbCBD.csv

## Execution
2. Point the generated file in src/org/fog/mobilitydata/DataParser.java
3. Run perfeval/MicroserviceApp_RandomMobility_Clustering.java
4. View output in dataset/output_data.csv

## Citation
```
@InProceedings{10.1007/978-3-031-19945-5_3,
author="Martins, Victoria B.
and de Macedo, Douglas D. J.
and Pioli, La{\'e}rcio
and Immich, Roger",
editor="Barolli, Leonard",
title="A Cluster Formation Algorithm for Fog Architectures Based on Mobility Parameters at a Geographically LAN Perspective",
booktitle="Advances on P2P, Parallel, Grid, Cloud and Internet Computing",
year="2023",
publisher="Springer International Publishing",
address="Cham",
pages="25--36",
}
```


> Tip: Using Eclipse IDE? You can create a launch group to run an application multiple times without executing one by one. Run as >  Run Configurations > Launch Group > (right click) New Configuration > Add > (select the desired .java) > Launch mode (Run) > Post launch action (Wait until terminated) > OK
