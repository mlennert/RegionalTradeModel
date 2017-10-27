# Code for a spatial interaction model using GRASS GIS to estimate regional exports based on individual firm data

This repository contains code of different successif versions and trials of constructing a model for estimating regional exports using GRASS GIS.

The two indicator versions are simple first attempts, which have since been superceded by the doubly-constrained spatial interaction model.

All are based on individual firm locations and the spatial interaction model additionally needs national input-output tables as input. The code can be run either on a single CPU or on a single machine with several CPUs. Personally, I ran the full model as a job array of single CPU jobs in a cluster computing environment.

Most papers linked to this code are still in review or print. I will add the references as soon as they become available. They should make the code more understandable. Some earlier references provide some of the aspects aready, however.

## References 

* Lennert, M., 2015. The Use of Exhaustive Micro-Data Firm Databases for Economic Geography: The Issues of Geocoding and Usability in the Case of the Amadeus Database. ISPRS International Journal of Geo-Information 4, 62–86. doi:10.3390/ijgi4010062
* Lennert, M., 2015. Approaching regional openness through measures of specialization and spatial market shares: experimentations with micro-data on enterprises. Plurimondi. An International Forum for Research and Debate on Human Settlements 8.
