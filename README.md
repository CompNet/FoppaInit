FoppaInit v1.0.2
-------------------------------------------------------------------------
*Initialization of the FOPPA database*

* Copyright 2021-2023 Lucas Potin & Vincent Labatut

FoppaInit is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation. For source availability and license information see `licence.txt`

* **Lab site:** http://lia.univ-avignon.fr
* **GitHub repo:** https://github.com/CompNet/FoppaInit
* **Data:** https://doi.org/10.5281/zenodo.7808664
* **Contact:** Lucas Potin <lucas.potin@univ-avignon.fr>
 
-------------------------------------------------------------------------

# Description
These scripts create the FOPPA database v.1.1.1 from raw TED files. This database relies mainly on the award notices of public contracts related to French clients and suppliers from 2010 to 2020 in the Tenders Electronic Daily. It also proposes an enrichment of these data, thanks to the siretization of agents (i.e. the retrieval of their unique IDs, which is missing for most of them) as well as the cleaning and extraction of award criteria, and other processing.

The process conducted to build the FOPPA is quite long, though (around 1 week, depeding on the hardware), so the produced database is alternatively directly available on [Zenodo](https://doi.org/10.5281/zenodo.7808664). The detail of this processing are described in a technical report [P'22].

This work was conducted in the framework of the [DeCoMaP](https://anr.fr/Projet-ANR-19-CE38-0004) ANR project (*Detection of corruption in public procurement markets* -- `ANR-19-CE38-0004`). If you use this source code or the produced database, please cite bibliographical reference [P'22].

# Organization
This repository is composed of the following elements:
* `requirements.txt` : List of Python packages used in foppaInit.py.
* `foppaInit.py` : Python script in order to create the FOPPA Database.
* `data` : folder with the input files needed to create the database.

The script requires the TED data and leverages some additional sources:
* TED : https://data.europa.eu/data/datasets/ted-1/
* hexaposte : https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/
* SIRENE : https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/
* GeoSIRENE : https://www.data.gouv.fr/fr/datasets/geolocalisation-des-etablissements-du-repertoire-sirene-pour-les-etudes-statistiques/

# Installation
You first need to install `python` and the required packages:
1. Install the Python language: https://www.python.org
2. Download this project from GitHub and unzip.
3. Install CUDA: https://developer.nvidia.com/cuda-downloads
4. Execute `pip install -r requirements.txt` to install some of the required packages (see also the *Dependencies* Section).

Package `blazingsql` requires a specific installation:
1. Go to Webpage https://rapids.ai/start.html
2. Configure your setup in this Webpage.
3. Execute the generated command.

# Use
In order to build the FOPPA database:
1. Open the Python console.
2. Run `foppaInit.py`.

The script is going to perform several tasks:
1. Download all the necessary data (see Section *Organization*).
2. Apply the processing described in [P'22].
3. Export the resulting database under different forms (SQL dump, CSV sheets).

# Dependencies
Tested with Python version 3.8.0, with the following packages:
* [`sqlite3`](https://www.sqlite.org/releaselog/3_39_4.html): version 3.39.4
* [`pandas`](https://pypi.org/project/pandas/): version 1.3.5.
* [`numpy`](https://pypi.org/project/numpy/): version 1.22.4.
* [`blazingsql`](https://rapids.ai/start.html): version 21.08.
* [`rapidfuzz`](https://pypi.org/project/rapidfuzz/): version 2.11.1.
* [`dedupe`](https://pypi.org/project/dedupe/): version 2.0.19.

# Data
The produced database is directly available publicly online on [Zenodo](https://doi.org/10.5281/zenodo.7443842), under three different forms:
* SQLite file: https://www.sqlite.org/index.html
* SQL dump.
* CSV files (one by table).

# References
**[P'22]** L. Potin, V. Labatut, R. Figueiredo, C. Largeron & P. H. Morand. *FOPPA: A database of French Open Public Procurement Award notices*, Technical Report, Avignon Université, 2022. [⟨hal-03796734⟩](https://hal.archives-ouvertes.fr/hal-03796734)
