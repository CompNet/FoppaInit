## FoppaInit
-------------------------------------------------------------------------
* Initialization of the FOPPA database

* Copyright 2021-2022 Lucas Potin & Vincent Labatut

Foppa is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation. For source availability and license information see `licence.txt`

* **Lab site:** http://lia.univ-avignon.fr
* **GitHub repo:** https://github.com/CompNet/FoppaInit
* **Contact:** Lucas Potin <lucas.potin@univ-avignon.fr>
 
-------------------------------------------------------------------------

## Description
These scripts are meant to create the Foppa database from raw TED files. Foppa contains the award notices of public contracts related to French clients from 2010 to 2020 in the Tenders Electronic Daily. Foppa proposes an enrichment of these data, thanks to the siretization of agents as well as the cleaning and extraction of award criteria.

## Organization
The project is composed of the following folders:

Here are the third-party softwares used in this version:
* hexaposte : https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/
* SIRENE : https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/

# Use


# Dependencies
Tested with `python` version 4.0.5, with the following packages:
* [`sqlite3`]: version 1.0.5.
* [`pandas`](https://pypi.org/project/pandas/): version 1.1.54.
* [`numpy`](https://pypi.org/project/numpy/): version 2.1.0.
* [`blazingsql`](https://rapids.ai/start.html): version 1.13.0.
* [`rapidfuzz`](https://pypi.org/project/rapidfuzz/): version 1.0.16.


