## Foppa Initialization

"""Imports"""
import math
import json
import sqlite3
import pandas as pd
import numpy as np 
import os
import re
import time
from os import walk
from datetime import date
#from blazingsql import BlazingContext
from rapidfuzz import fuzz
from rapidfuzz import process
import logging
import optparse
import dedupe
from unidecode import unidecode
import csv
import urllib.request
import shutil
import zipfile






def downloadFiles():
    """Download of the various files in the FOPPA database"""
    os.mkdir("data/contractNotices")  #Contract notices from TED
    os.mkdir("data/contractAwards")   #Contract award from TED
    os.mkdir("data/opening")          #Opening dates from SIRENE
    os.mkdir("data/Etab")             #All facilities from SIRENE
    os.mkdir("data/geolocate")        #For localisation of SIRENE entities
    
    #### Contract award
    urls = ["https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-2010.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-2011.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-2012.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-2013.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-2014.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-2015.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-2016.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-2017.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-2018.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-2019.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-2020.zip",
            ]

    for url in urls:
        urllib.request.urlretrieve(url,"Award.zip")
        with zipfile.ZipFile("Award.zip", 'r') as zip_ref:
            zip_ref.extractall("data/contractAwards")
    #### Contract Notices
    
    urls = ["https://data.europa.eu/api/hub/store/data/ted-contract-notices-2008.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-notices-2009.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-notices-2010.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-notices-2011.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-notices-2012.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-notices-2013.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-notices-2014.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-notices-2015.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-notices-2016.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-notices-2017.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-notices-2018.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-notices-2019.zip",
            "https://data.europa.eu/api/hub/store/data/ted-contract-notices-2020.zip"
            ]
    
    for url in urls:
        urllib.request.urlretrieve(url,"Contract.zip")
        with zipfile.ZipFile("Contract.zip", 'r') as zip_ref:
            zip_ref.extractall("data/contractNotices")
            
    os.remove("Contract.zip")
    os.remove("Award.zip")
    ######SIRENE:
    
    #Opening Dates
    urls = ["https://files.data.gouv.fr/insee-sirene/StockEtablissementHistorique_utf8.zip"]
    for url in urls:
        urllib.request.urlretrieve(url,"HistoEtab.zip")
        with zipfile.ZipFile("HistoEtab.zip", 'r') as zip_ref:
            zip_ref.extractall("data/opening")
            
    chunksize = 10 ** 6
    compteur=0
    compt=-1
    with pd.read_csv("data/opening/StockEtablissementHistorique_utf8.csv", chunksize=chunksize,dtype="str") as reader:
        for chunk in reader:
            if compt>-1:
                compt=compt+1 
                chunk = chunk[["siret","dateDebut","dateFin"]]
                name = "data/opening/HistoPart"+str(compteur)+".csv"
                datefin = np.array(chunk["dateFin"])
                for j in range(len(datefin)):
                    if len(str(datefin[j]))<4:
                        datefin[j]="2030-01-01"
                chunk["dateFin"]=datefin
                chunk.to_csv(name,index=False)   
                compteur=compteur+1 
    os.remove("data/opening/StockEtablissementHistorique_utf8.csv")
    os.remove("HistoEtab.zip")
    
    #Facilities 
    
    urls = ["https://files.data.gouv.fr/insee-sirene/StockEtablissement_utf8.zip",
    "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip"]
    for url in urls:
        urllib.request.urlretrieve(url,"Sirene.zip")
        with zipfile.ZipFile("Sirene.zip", 'r') as zip_ref:
            zip_ref.extractall("data/Etab")
    
    os.remove("Sirene.zip")
    chunksize = 10 ** 6
    compteur=0    
    allCol = ["siret","siren","nomEnseigne","nomUnite","num","typevoie","libelle","cp","ville","TypeActivite","CatJuridique"]
    chunksize = 10 ** 6
    newDF = pd.DataFrame(index=range(0,chunksize),columns=allCol)
    diconom = {}
    dicotype = {}
    filename = "data/Etab/StockUniteLegale_utf8.csv"
    chunksize = 10 ** 6
    compt=-1
    for chunk in pd.read_csv(filename, chunksize=chunksize,dtype = str):
        if compt>-2:
            compt=compt+1
            for l in range(len(chunk)):
                    if not(str(chunk["denominationUniteLegale"][chunksize*compt+l]) == "nan"):
                            temp = str(chunk["denominationUniteLegale"][chunksize*compt+l])
                    elif not(str(chunk["denominationUsuelle1UniteLegale"][chunksize*compt+l]) == "nan"):
                            temp = str(chunk["denominationUsuelle1UniteLegale"][chunksize*compt+l])
                    elif not(str(chunk["denominationUsuelle2UniteLegale"][chunksize*compt+l]) == "nan"):
                            temp = str(chunk["denominationUsuelle2UniteLegale"][chunksize*compt+l])
                    elif not(str(chunk["denominationUsuelle3UniteLegale"][chunksize*compt+l]) == "nan"):
                            temp = str(chunk["denominationUsuelle3UniteLegale"][chunksize*compt+l])
                    else:
                            temp = str(chunk["prenom1UniteLegale"][chunksize*compt+l]) + " " + str(chunk["nomUniteLegale"][chunksize*compt+l])
                    diconom[str(chunk["siren"][chunksize*compt+l])] = temp
                    dicotype[str(chunk["siren"][chunksize*compt+l])] = str(chunk["categorieJuridiqueUniteLegale"][chunksize*compt+l])
    numeroFichier=0
    
    
    os.remove("data/opening/StockUniteLegale_utf8.csv")
    
    
    filename = "data/Etab/StockEtablissement_utf8.csv"
    chunksize = 10 ** 6
    compt=-1
    for chunk in pd.read_csv(filename, chunksize=chunksize,dtype = str):
        a=0
        # Insert a row of data
        compt=compt+1
        for l in range(len(chunk)):
            siren = str(chunk["siren"][chunksize*compt+l])
            siret = str(chunk["siret"][chunksize*compt+l])
            num = str(chunk["numeroVoieEtablissement"][chunksize*compt+l])
            typevoie = str(chunk["typeVoieEtablissement"][chunksize*compt+l])
            libelle = str(chunk["libelleVoieEtablissement"][chunksize*compt+l])
            cp = str(chunk["codePostalEtablissement"][chunksize*compt+l])
            ville = str(chunk["libelleCommuneEtablissement"][chunksize*compt+l])
            e1=  str(chunk["enseigne1Etablissement"][chunksize*compt+l])
            denom=  str(chunk["denominationUsuelleEtablissement"][chunksize*compt+l])
            e2=  str(chunk["enseigne2Etablissement"][chunksize*compt+l])
            e3=  str(chunk["enseigne3Etablissement"][chunksize*compt+l])
            nom = diconom.get(siren,"0")
            typ = dicotype.get(siren,"0")
            activite = str(chunk["activitePrincipaleEtablissement"][chunksize*compt+l])
            if not(e1=="nan"):
                nome = e1
            elif not(denom=="nan"):
                nome = denom
            elif not(e2=="nan"):
                nome = e2
            elif not(e3=="nan"):
                nome = e3
            else:
                nome = nom
            params = (siret,siren,nome,nom,num,typevoie,libelle,cp,ville,activite,typ)
            newDF.iloc[a] = params
            a = a+1
            del siren,siret,num,typevoie,libelle,cp,ville,nom,nome,e1,denom,e2,e3
        newDF["ville"] = newDF["ville"].replace(regex=r"[0-9]+",value=r"")
        newDF["ville"] = newDF["ville"].replace(regex=r"CEDEX",value=r"")
        newDF["ville"] = newDF["ville"].replace(regex=r"-",value=r" ")
        newDF["ville"] = newDF["ville"].replace(regex=r"'",value=r" ")
        newDF["ville"] = newDF["ville"].str.lstrip()
        newDF["ville"] = newDF["ville"].str.rstrip()
        nomFD = "data/Etab/EtabPart"+str(numeroFichier)+".csv"
        newDF.to_csv(nomFD,index=False)
        numeroFichier = numeroFichier+1
        newDF = pd.DataFrame(index=range(0,chunksize),columns=allCol)

    os.remove("data/opening/StockEtablissement_utf8.csv")
    datas = pd.read_csv("data/Etab/EtabPart0.csv",dtype=str)

    for l in range(len(datas)):
        if datas["nomEnseigne"][l] == "MAIRIE":
            temp = datas["ville"][l]
            temp = "MAIRIE DE "+datas["ville"][l]
            datas["nomEnseigne"][l] = temp
    datas.to_csv("data/Etab/EtabPart0.csv",index=False)
    
    #Geolocalisation 
    urls = ["https://files.data.gouv.fr/insee-sirene-geo/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.zip"]
    for url in urls:
        urllib.request.urlretrieve(url,"Sirene.zip")
        with zipfile.ZipFile("Sirene.zip", 'r') as zip_ref:
            zip_ref.extractall("data/geolocate")        
    
    
def databaseCreation(nameDatabase):
    """Creation of the tables of the database"""
    database = sqlite3.connect(nameDatabase)
    cursor = database.cursor()
    request = "DROP TABLE IF EXISTS Lots"
    sql = cursor.execute(request)
    request = "CREATE TABLE Lots(lotId INTEGER,tedCanId INTEGER,correctionsNB INTEGER,cancelled INTEGER,awardDate TEXT,awardEstimatedPrice NUMERIC,awardPrice NUMERIC,cpv TEXT,tenderNumber INTEGER,onBehalf TINYINT,jointProcurement TINYINT,fraAgreement TINYINT,fraEstimated INTEGER,lotsNumber INTEGER,accelerated TINYINT,outOfDirectives TINYINT,contractorSme TINYINT,numberTendersSme INTEGER,subContracted TINYINT,gpa	TINYINT,multipleCae	TINYINT,typeOfContract TEXT,topType	TEXT,renewal TINYINT, contractDuration INTEGER, publicityDuration INTEGER,PRIMARY KEY(lotId))"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS AgentsBase"
    sql = cursor.execute(request)
    request = "CREATE TABLE AgentsBase(agentId INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode	TEXT,country TEXT, date TEXT,type TEXT,PRIMARY KEY(agentId))"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS Agents"
    sql = cursor.execute(request)
    request = "CREATE TABLE Agents(agentId INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode	TEXT,country TEXT, department TEXT,PRIMARY KEY(agentId))"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS AgentsLink"
    sql = cursor.execute(request)
    request = "CREATE TABLE AgentsLink(temporalagentId INTEGER,agentId INTEGER)"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS CriteriaTemp"
    sql = cursor.execute(request)
    request = "CREATE TABLE CriteriaTemp (lotId INTEGER,CRIT_PRICE_WEIGHT TEXT,CRIT_WEIGHTS TEXT, CRIT_CRITERIA TEXT)"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS LotClients"
    sql = cursor.execute(request)
    request = "CREATE TABLE LotClients(lotId INTEGER,agentId INTEGER,FOREIGN KEY(agentId) REFERENCES Agents(agentId) ON UPDATE CASCADE,FOREIGN KEY(lotId) REFERENCES Lots(lotId) ON UPDATE CASCADE)"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS LotSuppliers"
    sql = cursor.execute(request)
    request = "CREATE TABLE LotSuppliers(lotId INTEGER,agentId INTEGER,FOREIGN KEY(agentId) REFERENCES Agents(agentId) ON UPDATE CASCADE,FOREIGN KEY(lotId) REFERENCES Lots(lotId) ON UPDATE CASCADE)"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS Names"
    sql = cursor.execute(request)
    request = "CREATE TABLE Names(agentId INTEGER,name TEXT)"
    sql = cursor.execute(request)
    database.commit()
    return database

def load_csv_files():
    """Extraction of the csv files into the database"""
    listeFichiers = []
    newDF = []
    for (repertoire, sousRepertoires, fichiers) in walk("data/contractAwards"):
        listeFichiers.extend(fichiers)
    for j in listeFichiers:
        datas = pd.read_csv("data/contractAwards/"+j,dtype=str)
        # We only keep french contracts
        newDF.append(datas[datas["ISO_COUNTRY_CODE"].str.contains("FR")])
    result = pd.concat(newDF)
    return result

def firstCleaning(datas,database):
    """Insert into database + some preliminar cleaning"""
    columns = datas.columns
    # First cleaning : Normalize + Upper strings
    for column in columns:
        datas[column] = datas[column].str.upper()
        datas[column]= datas[column].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    
    # Parenthesis deletion
    datas["CAE_NAME"] = datas["CAE_NAME"].replace(regex=r'\([^)]*\)',value=r"")
    
    nameCAE = np.array(datas["CAE_NAME"])
    siretCAE = np.array(datas["CAE_NATIONALID"])
    addressCAE = np.array(datas["CAE_ADDRESS"])
    townCAE = np.array(datas["CAE_TOWN"])
    postalCodeCAE = np.array(datas["CAE_POSTAL_CODE"])
    countryCAE = np.array(datas["ISO_COUNTRY_CODE"])
    nameWIN = np.array(datas["WIN_NAME"])
    siretWIN = np.array(datas["WIN_NATIONALID"])
    addressWIN = np.array(datas["WIN_ADDRESS"])
    townWIN = np.array(datas["WIN_TOWN"])
    postalCodeWIN = np.array(datas["WIN_POSTAL_CODE"])
    countryWIN = np.array(datas["WIN_COUNTRY_CODE"])
    tedCanId = np.array(datas["ID_NOTICE_CAN"])
    correctionsNB = np.array(datas["CORRECTIONS"])
    cancelled = np.array(datas["CANCELLED"])
    awardEstimatedPrice = np.array(datas["AWARD_EST_VALUE_EURO"])
    awardPrice = np.array(datas["AWARD_VALUE_EURO_FIN_1"])
    cpv = np.array(datas["CPV"])
    tenderNumber = np.array(datas["NUMBER_OFFERS"])
    onBehalf = np.array(datas["B_ON_BEHALF"])
    jointProcurement = np.array(datas["B_INVOLVES_JOINT_PROCUREMENT"])
    fraAgreement = np.array(datas["B_FRA_AGREEMENT"])
    fraEstimated = np.array(datas["FRA_ESTIMATED"])
    lotsNumber = np.array(datas["ID_LOT_AWARDED"])
    accelerated = np.array(datas["B_ACCELERATED"])
    outOfDirectives = np.array(datas["OUT_OF_DIRECTIVES"])
    contractorSme = np.array(datas["B_CONTRACTOR_SME"])
    numbersTendersSme = np.array(datas["NUMBER_TENDERS_SME"])
    subContracted = np.array(datas["B_SUBCONTRACTED"])
    gpa = np.array(datas["B_GPA"])
    multipleCae = np.array(datas["B_MULTIPLE_CAE"])
    typeofContract = np.array(datas["TYPE_OF_CONTRACT"])
    topType = np.array(datas["TOP_TYPE"])
    criteria = np.array(datas["CRIT_CRITERIA"])
    criteriaW = np.array(datas["CRIT_WEIGHTS"])
    criteriaP = np.array(datas["CRIT_PRICE_WEIGHT"])

    ### Change date format
    dico = {"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06","JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}
    for key in dico:
        datas["DT_AWARD"] = datas["DT_AWARD"].replace(regex=key,value=dico[key])
        datas["DT_DISPATCH"] = datas["DT_DISPATCH"].replace(regex=key,value=dico[key])
        
    awardDate = np.array(datas["DT_AWARD"])
    dispatchDate = np.array(datas["DT_DISPATCH"])
    
    for i in range(len(awardDate)):
        if len(str(awardDate[i]))>4:
            temp = awardDate[i].split("-")
            awardDate[i] = "20"+temp[2]+"-"+temp[1]+"-"+temp[0]
        if len(str(dispatchDate[i]))>4:
            temp = dispatchDate[i].split("-")
            dispatchDate[i] = "20"+temp[2]+"-"+temp[1]+"-"+temp[0]

    cur = database.cursor()
    compteurAgent=0
    for i in range(len(datas)):
        sql = ''' INSERT INTO CriteriaTemp(lotId,CRIT_PRICE_WEIGHT,CRIT_WEIGHTS, CRIT_CRITERIA)
                  VALUES (?,?,?,?) '''
        val =(i,criteriaP[i],criteriaW[i],criteria[i])
        cur.execute(sql,val)
        
        sql = ''' INSERT INTO Lots(lotId,tedCanId,correctionsNB,cancelled,awardDate,awardEstimatedPrice,awardPrice,cpv,tenderNumber,onBehalf,jointProcurement,fraAgreement,fraEstimated,lotsNumber,accelerated,outOfDirectives,contractorSme,numberTendersSme,subContracted,gpa,multipleCae,typeOfContract,topType)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
        val = (i,tedCanId[i],correctionsNB[i],cancelled[i],awardDate[i],awardEstimatedPrice[i],awardPrice[i],cpv[i],tenderNumber[i],onBehalf[i],jointProcurement[i],fraAgreement[i],fraEstimated[i],lotsNumber[i],accelerated[i],outOfDirectives[i],contractorSme[i],numbersTendersSme[i],subContracted[i],gpa[i],multipleCae[i],typeofContract[i],topType[i])
        cur.execute(sql,val)
        date = None
        if len(str(awardDate[i]))>4:
            date = awardDate[i]
        else:
            date = dispatchDate[i]
        names = nameCAE[i].split("---")
        sirets = None
        addresses = None
        towns = None
        pcs = None
        countrys = None
        if not(str(siretCAE[i])=="nan"):
            sirets = siretCAE[i].split("---")
        if not(str(addressCAE[i])=="nan"):
            addresses = addressCAE[i].split("---")
        if not(str(townCAE[i])=="nan"):
            towns = townCAE[i].split("---") 
        if not(str(postalCodeCAE[i])=="nan"):
            pcs = postalCodeCAE[i].split("---")
        if not(str(countryCAE[i])=="nan"):
            countrys = countryCAE[i].split("---")
        for k in range(len(names)):
            tempName = names[k]
            tempSiret = None
            tempAddress = None
            tempTown = None
            tempPC = None
            tempCountry = None
            if not(sirets==None) and k<len(sirets):
                tempSiret = sirets[k]
            if not(addresses==None) and k<len(addresses):
                tempAddress = addresses[k]
            if not(towns==None) and k<len(towns):
                tempTown = towns[k]
            if not(pcs==None) and k<len(pcs):
                tempPC = pcs[k]
            if not(countrys==None) and k<len(countrys):
                tempCountry = countrys[k]
            sql = ''' INSERT INTO AgentsBase(agentId,name,siret,address,city,zipcode,country,date,type)
                VALUES (?,?,?,?,?,?,?,?,?)'''
            val = (compteurAgent,tempName,tempSiret,tempAddress,tempTown,tempPC,tempCountry,date,"CAE")
            cur.execute(sql,val)
            sql = ''' INSERT INTO LotClients(lotId,agentId)
                VALUES (?,?)'''
            val = (i,compteurAgent)
            cur.execute(sql,val)
            compteurAgent = compteurAgent+1
        
        names=["INFRUCTUEUX"]
        if not(str(nameWIN[i])=="nan"):
            names = nameWIN[i].split("---")
            names2 = nameWIN[i].split("/")
        sirets = None
        addresses = None
        towns = None
        pcs = None
        countrys = None
        if len(names)>1 or len(names2)<2:
            if not(str(siretWIN[i])=="nan"):
                sirets = siretWIN[i].split("---")
            if not(str(addressWIN[i])=="nan"):
                addresses = addressWIN[i].split("---")
            if not(str(townWIN[i])=="nan"):
                towns = townWIN[i].split("---") 
            if not(str(postalCodeWIN[i])=="nan"):
                pcs = postalCodeWIN[i].split("---")
            if not(str(countryWIN[i])=="nan"):
                countrys = countryWIN[i].split("---")
        else:
            names=names2
            if not(str(siretWIN[i])=="nan"):
                sirets = siretWIN[i].split("/")
            if not(str(addressWIN[i])=="nan"):
                addresses = addressWIN[i].split("/")
            if not(str(townWIN[i])=="nan"):
                towns = townWIN[i].split("/") 
            if not(str(postalCodeWIN[i])=="nan"):
                pcs = postalCodeWIN[i].split("/")
            if not(str(countryWIN[i])=="nan"):
                countrys = countryWIN[i].split("/")
        for k in range(len(names)):
            tempName = names[k]
            tempSiret = None
            tempAddress = None
            tempTown = None
            tempPC = None
            tempCountry = None
            if not(sirets==None) and k<len(sirets):
                tempSiret = sirets[k]
            if not(addresses==None) and k<len(addresses):
                tempAddress = addresses[k]
            if not(towns==None) and k<len(towns):
                tempTown = towns[k]
            if not(pcs==None) and k<len(pcs):
                tempPC = pcs[k]
            if not(countrys==None) and k<len(countrys):
                tempCountry = countrys[k]
            sql = ''' INSERT INTO AgentsBase(agentId,name,siret,address,city,zipcode,country,date,type)
                    VALUES (?,?,?,?,?,?,?,?,?)'''
            val = (compteurAgent,tempName,tempSiret,tempAddress,tempTown,tempPC,tempCountry,date,"WIN")
            cur.execute(sql,val)
            sql = ''' INSERT INTO LotSuppliers(lotId,agentId)
                    VALUES (?,?)'''
            val = (i,compteurAgent)
            cur.execute(sql,val)
            compteurAgent = compteurAgent+1
    database.commit()
    return database

def mainCleaning(database):
    datas = pd.read_sql_query("SELECT * FROM AgentsBase", database,dtype=str) 
    ###Nettoyage des donnees
    ##Nettoyage du Nom
    datas["name"] = datas["name"].str.replace("'"," ")
    datas["name"] = datas["name"].str.replace("-"," ")
    datas["name"] = datas["name"].replace(regex=r"&APOS;",value=r" ")
    datas["name"] = datas["name"].replace(regex=r";[A-Z, ]+",value=r"")
    datas["name"] = datas["name"].replace(regex=r"- [A-Z, ]+",value=r"")
    datas["name"] = datas["name"].replace(regex=r'\([^)]*\)',value=r"")
    datas["name"] = datas["name"].replace(regex=r"-",value=r" ")
    datas["name"] = datas["name"].replace(regex=r"'",value=r" ")
    datas["name"] = datas["name"].str.lstrip()
    datas["name"] = datas["name"].str.rstrip()

    #Nettoyage du SIRET
    datas["siret"]=datas["siret"].replace(regex='[^\d]+',value="")
    datas["siret"] = datas["siret"].str.lstrip()
    datas["siret"] = datas["siret"].str.rstrip() 

    sirets = np.array(datas["siret"])
    for i in range(len(sirets)):
        if not(len(str(sirets[i]))==14):
            sirets[i]="NULL_IDENTIFIER"
    datas["siret"]=sirets
    

    #Nettoyage de l'adresse
    datas["address"] = datas["address"].replace(regex=r"&APOS;",value=r" ")
    datas["address"] = datas["address"].replace(regex=r"&AMP",value=r"&")
    datas["address"] = datas["address"].replace(regex=r"(\d);",value=r"\1 ")
    datas["address"] = datas["address"].replace(regex=r"(\d)-(\d):",value=r"\1 ")
    datas["address"] = datas["address"].replace(regex=r"(\d)/(\d)",value=r"\1 ")
    datas["address"] = datas["address"].replace(regex=r"(\d);(\d)",value=r"\1 ")
    datas["address"] = datas["address"].replace(regex="BIS",value="")
    datas["address"] = datas["address"].replace(regex=r"([A-Z])-([A-Z])",value=r"\1 \2")
    datas["address"] = datas["address"].replace(regex="'",value=" ")
    datas["address"] = datas["address"].replace(regex=";",value=" ; ")
    datas["address"] = datas["address"].replace(regex=r"(^[A-Z])",value=r" \1")
    datas["address"] = datas["address"].replace(regex=r" +",value=r" ")
    datas["address"] = datas["address"].replace(regex=r" BP [0-9, ]+",value=r"")
    datas["address"] = datas["address"].replace(regex=r" CEDEX [0-9, ]+",value=r" ")
    datas["address"] = datas["address"].replace(regex=r" CS [0-9, ]+",value=r" ")
    datas["address"] = datas["address"].str.lstrip()
    datas["address"] = datas["address"].str.rstrip()
    ### Nettoyage Partie Ville
    replaceVille = {"CEDEX":"","-":" "," SP ":"","&APOS;":" ","'":" ",'\d':""}
    for key in replaceVille:
        datas["city"]=datas["city"].replace(regex=key,value=replaceVille[key])
    datas["city"] = datas["city"].str.lstrip()
    datas["city"] = datas["city"].str.rstrip()
    datas["city"] = datas["city"].replace(regex=r"\)",value=r"")
    datas["city"] = datas["city"].replace(regex=r"\(",value=r"")

    #Nettoyage du code Postal
    datas["zipcode"]=datas["zipcode"].replace(regex='[^\d]+',value="")
    datas["zipcode"] = datas["zipcode"].str.lstrip()
    datas["zipcode"] = datas["zipcode"].str.rstrip() 

    zips = np.array(datas["zipcode"])
    for i in range(len(zips)):
        if not(len(str(zips[i]))==5):
            zips[i]=None
    datas["zipcode"]=zips
    datas['name'] = datas['name'].fillna("NULL_IDENTIFIER")
    datas['address'] = datas['address'].fillna("NULL_IDENTIFIER")
    datas['city'] = datas['city'].fillna("NULL_IDENTIFIER")
    datas2 = datas.groupby(["name",'address','city','siret'],as_index=False).agg({'agentId':'-'.join,'name':'first','siret':'first','address':'first','city':'first','zipcode':'first','country':'first','date':'first','type':'first'})
    datas = datas2
    cursor = database.cursor()
    request = "DROP TABLE IF EXISTS AgentsTemp"
    sql = cursor.execute(request)
    request = "CREATE TABLE IF NOT EXISTS AgentsTemp(agentId INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode	TEXT,country TEXT, date TEXT,ids TEXT,type TEXT,PRIMARY KEY(agentId))"
    sql = cursor.execute(request)

    types = np.array(datas["type"]) 
    ids = np.array(datas["agentId"]) 
    names = np.array(datas["name"]) 
    sirets = np.array(datas["siret"]) 
    addresses = np.array(datas["address"]) 
    citys = np.array(datas["city"]) 
    zipcodes = np.array(datas["zipcode"]) 
    countrys = np.array(datas["country"]) 
    dates = np.array(datas["date"]) 

    for i in range(len(ids)):
        sql = ''' INSERT INTO AgentsTemp(agentId,name,siret,address,city,zipcode,country,date,ids,type)
                VALUES (?,?,?,?,?,?,?,?,?,?)'''
        val = (i,names[i],sirets[i],addresses[i],citys[i],zipcodes[i],countrys[i],dates[i],ids[i],types[i])
        cursor.execute(sql,val)
    database.commit()
    return database

def fineTuningAgents(database):
    datas = pd.read_sql_query("SELECT * FROM AgentsTemp", database,dtype=str) 
    cursor = database.cursor()
    ## La table a siretiser au final : 
    request = "DROP TABLE IF EXISTS AgentsSiretiser"
    sql = cursor.execute(request)
    request = "CREATE TABLE IF NOT EXISTS AgentsSiretiser(agentId INTEGER,name TEXT,siret TEXT,address TEXT,newAddress TEXT,city TEXT,zipcode TEXT,country TEXT, date TEXT,catJuridique TEXT,company TEXT,ids TEXT,type TEXT, PRIMARY KEY(agentId))"
    sql = cursor.execute(request)

    ### 1 : Supprimer ce qu'on ne veut pas siretiser
    name = np.array(datas["name"])
    country = np.array(datas["country"])
    delete = ["INFRUCT","SANS SUITE","ABANDON","SANS OBJET","EN ATTENTE","AUCUNE OFFRE"]
    for i in range(len(name)):
        for pattern in delete:
            if pattern in name[i]:
                country[i]="INFRUCTUEUX"
    datas["country"] = country

    ### 2 : Hexaposte : 
    pcs = np.array(datas["zipcode"])
    hexaposte = pd.read_csv("data/foppaFiles/hexaposte.csv",dtype=str,sep=";")
    hexaposte["libelle_d_acheminement"] = hexaposte["libelle_d_acheminement"].str.replace("-"," ")


    citys = np.array(datas["city"])
    country = np.array(datas["country"])
    for i in range(len(citys)):
        if not(len(str(pcs[i]))==5) and not(str(citys[i])=="NULL_IDENTIFIER"): ##Hexaposte may be usefull
            temp = hexaposte[hexaposte["libelle_d_acheminement"]==citys[i]]
            temp = temp.reset_index()
            if len(temp)>0:
                if country[i] =="FR" or str(country[i])=="nan":
                    pcs[i] = temp["code_postal"][0]
                    country[i] = "FR"
    datas["zipcode"] = pcs
    datas["country"] = country
    datas.assign(newCP=0)
    datas["newCP"]=pcs
    ##No search on countries 
    siret = np.array(datas["siret"])
    country = np.array(datas["country"])
    pcs = np.array(datas["zipcode"])
    for i in range(len(pcs)):
        country[i] = str(country[i])
        if not(country[i] == "None" or country[i] == "nan" or country[i] == "FR" or country[i] == "PF" or country[i] == "WF" or country[i] == "NC" or country[i] == "YT" or country[i] == "RE" or country[i] == "GF" or country[i] == "MQ" or country[i] == "GP"):
            pcs[i] = None
            siret[i] = None
    datas["zipcode"] = pcs
    datas["siret"] = siret
    #datas.to_csv("AgentHexaposted.csv",index=False)  
    datas["newCP"]=pcs
    ##Fine Tuning du nom
    datas["name"] = datas["name"].replace(regex=r"CONSEIL DEPARTEMENTAL",value=r"DEPARTEMENT")
    datas["name"] = datas["name"].replace(regex=r"CONSEIL GENERAL",value=r"DEPARTEMENT")
    datas["name"] = datas["name"].replace(regex=r"CONSEIL REGIONAL",value=r"REGION")
    datas["name"] = datas["name"].replace(regex=r"^CHU ",value=r"CENTRE HOSPITALIER UNIVERSITAIRE ")
    datas["name"] = datas["name"].replace(regex=r"^CH ",value=r"CENTRE HOSPITALIER ")
    datas["name"] = datas["name"].replace(regex=r"^CC D[U|E]",value=r"COMMUNAUTE DE COMMUNE ")
    datas["name"] = datas["name"].replace(regex=r"^CDC D[U|E]",value=r"COMMUNAUTE DE COMMUNE ")
    datas["name"] = datas["name"].replace(regex=r"^CA ",value=r"COMMUNAUTE D AGGLOMERATION ")
    datas["name"] = datas["name"].replace(regex=r"^VILLE D[U|E]",value=r"MAIRIE D ")
    datas["name"] = datas["name"].replace(regex=r"^COMMUNE D[U|E]",value=r"MAIRIE D ")
    datas["name"] = datas["name"].replace(regex=r"^CCAS ",value=r"CENTRE COMMUNAL D ACTION SOCIALE ")
    datas["name"] = datas["name"].replace(regex=r"^SIVU ",value=r"SYNDICAT INTERCOM ")
    datas["name"] = datas["name"].replace(regex=r"^CAF SA ",value=r" CAF SA ")
    datas["name"] = datas["name"].replace(regex=r"^CAF FRANCE ",value=r" CAF FRANCE")
    datas["name"] = datas["name"].replace(regex=r"^CAF ",value=r"CAISSE D ALLOCATION FAMILIALES ")
    convertisseur = pd.read_csv("data/foppaFiles/CatJuridique.csv",dtype=str,sep=";")
    datas = datas.assign(catJuridique=None)
    for i in range(len(convertisseur)):
        temp = convertisseur["Nom"][i]
        datas.loc[datas["name"].str.contains(temp,regex=True,na=False),'catJuridique' ] = convertisseur["catJuridique"][i]
    import copy as cp
        #### Adresses
    datasetEntites=cp.deepcopy(datas)
    libellesConvert = pd.read_csv("data/foppaFiles/Libelle.csv",dtype=str)
    for k in range(len(libellesConvert)): ##Transformation des types (Avenue--Av)
            Seekingpattern = " "+str(libellesConvert["Nom_Voie"][k]) + " "
            Appliedpattern = " "+str(libellesConvert["Nom_Voie_Nomalise"][k]) + " "
            datasetEntites["address"] = datasetEntites["address"].replace(regex=Seekingpattern,value=Appliedpattern)
    datasetEntites = datasetEntites.assign(NumVoie=0)
    datasetEntites = datasetEntites.assign(CodeVoie=0)  ##Separation de l'adresse en 3 (Num + Type + Libelle)
    datasetEntites = datasetEntites.assign(LibelVoie=0) 
    datasetEntites = datasetEntites.assign(NewAdress=0) 

    listeLibel = list(libellesConvert["Nom_Voie_Nomalise"]) 
    listeLibel = "|".join(listeLibel)
    ##### Les cas qu'on recherche : Libelle seul , type + libelle, num + type + libelle
    endingschar = ["-",";"," BP "," CS ","$","."]
    endingschar = "|".join(endingschar)

    datasetEntites["NewAdress"] = datasetEntites["address"]
    reg="(" +listeLibel + ") ([A-Z,0-9, ]+)("+endingschar+")"
    datasetEntites["NewAdress"] = datasetEntites["address"]
    datasetExtraction = datasetEntites["NewAdress"].str.extract(reg)
    datasetEntites["CodeVoie"] = datasetExtraction[0]
    datasetEntites["LibelVoie"] = datasetExtraction[1]


    ##Regex num+type+libelle
    datasetEntites["NewAdress"] = datasetEntites["address"]
    reg="([0-9]+) (" +listeLibel + ") ([A-Z,0-9, ]+)("+endingschar+")"
    datasetEntites["NewAdress"] = datasetEntites["address"]
    datasetExtraction = datasetEntites["NewAdress"].str.extract(reg)
    datasetEntites2 = datasetEntites.copy()
    datasetEntites2["NumVoie"] = datasetExtraction[0]
    datasetEntites2["CodeVoie"] = datasetExtraction[1]
    datasetEntites2["LibelVoie"] = datasetExtraction[2]
    adresses = []
    for i in range(len(datasetEntites2)):
        if not(str(datasetEntites2["CodeVoie"][i])=="nan") and not(str(datasetEntites2["LibelVoie"][i])=="nan"):
            adresses.append(str(datasetEntites2["NumVoie"][i])+" "+datasetEntites2["CodeVoie"][i]+" "+datasetEntites2["LibelVoie"][i])
        else:
            adresses.append(datasetEntites["address"][i])
    datas.assign(NewAdress=0)
    datas["NewAdress"] = adresses

    ids = np.array(datas["ids"]) 
    types = np.array(datas["type"]) 
    names = np.array(datas["name"]) 
    sirets = np.array(datas["siret"]) 
    addresses = np.array(datas["address"]) 
    Newaddresses = np.array(datas["NewAdress"]) 
    citys = np.array(datas["city"]) 
    zipcodes = np.array(datas["newCP"]) 
    countrys = np.array(datas["country"]) 
    dates = np.array(datas["date"]) 
    catJuridique = np.array(datas["catJuridique"]) 

    for i in range(len(ids)):
        sql = ''' INSERT INTO AgentsSiretiser(agentId,name,siret,address,newAddress,city,zipcode,country,date,catJuridique,ids,type)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''
        val = (i,names[i],sirets[i],addresses[i],Newaddresses[i],citys[i],zipcodes[i],countrys[i],dates[i],catJuridique[i],ids[i],types[i])
        cursor.execute(sql,val)
        
        tempID = str(ids[i]).split("-")[0]
        sql = ''' INSERT INTO Names(agentId,name)
                VALUES (?,?)'''
        val = (int(tempID),names[i])
        cursor.execute(sql,val)
        
    request = "UPDATE AgentsSiretiser SET name = NULLIF(name,'NULL_IDENTIFIER')"
    sql = cursor.execute(request)
    request = "UPDATE AgentsSiretiser SET siret = NULLIF(siret,'NULL_IDENTIFIER')"
    sql = cursor.execute(request)
    request = "UPDATE AgentsSiretiser SET address = NULLIF(address,'NULL_IDENTIFIER')"
    sql = cursor.execute(request)
    request = "UPDATE AgentsSiretiser SET city = NULLIF(city,'NULL_IDENTIFIER')"
    sql = cursor.execute(request)
    request = "UPDATE AgentsSiretiser SET zipcode = NULLIF(zipcode,'NULL_IDENTIFIER')"
    sql = cursor.execute(request)
    request = "UPDATE AgentsSiretiser SET name = NULLIF(name,'None')"
    sql = cursor.execute(request)
    request = "UPDATE AgentsSiretiser SET siret = NULLIF(siret,'None')"
    sql = cursor.execute(request)
    request = "UPDATE AgentsSiretiser SET address = NULLIF(address,'None')"
    sql = cursor.execute(request)
    request = "UPDATE AgentsSiretiser SET city = NULLIF(city,'None')"
    sql = cursor.execute(request)
    request = "UPDATE AgentsSiretiser SET zipcode = NULLIF(zipcode,'None')"
    sql = cursor.execute(request)
    request = "DELETE FROM AgentsSiretiser WHERE country='INFRUCTUEUX'"
    sql = cursor.execute(request)
    database.commit()
    return database



def safe_cast(val, to_type, default=None):
    try:
        return to_type(val)
    except (ValueError, TypeError):
        return default
    
def findType(chaine):
    """Matching between strings in Criteria and criteria Type"""
    lchaine = str.upper(chaine)
    lchaine = unidecode(lchaine)
    if "ENVIRONNEMENT" in lchaine: ##Specific case : Environnemental and Social aspect mixed together
        if "SOCIA" in lchaine:
            return("MIXTE")
    if "TECHNIQUE" in lchaine:
        return("TECHNICAL")
    if "TEHNIQUE" in lchaine:
        return("TECHNICAL")
    if "PRIX" in lchaine:
        return("PRICE")
    if "DELAI" in lchaine:
        return("DELAY")
    if "ENVIRONNEMENT" in lchaine:
        return("ENVIRONMENTAL")
    if "REMISE" in lchaine:
        return("PRICE")
    if "MONTANT" in lchaine:
        return("PRICE")
    if "ECONOMIQUE" in lchaine:
        return("PRICE")
    if "DURABLE" in lchaine:
        return("ENVIRONMENTAL")
    if "COUT" in lchaine:
        return("PRICE") 
    if "TARIF" in lchaine:
        return("PRICE")
    if "FINANCIER" in lchaine:
        return("PRICE")
    if "SOCIAL" in lchaine:
        return("SOCIAL")
    if "QUALITE" in lchaine:
        return("TECHNICAL")
    if "QUALITATI" in lchaine:
        return("TECHNICAL")
    if "HUMAIN" in lchaine:
        return("SOCIAL")
    if "PERSONNEL" in lchaine:
        return("SOCIAL")
    if "FONCTIONNALITE" in lchaine:
        return("TECHNICAL")
    return("OTHER")



def criteriaProcessing(database):
    """Split, clean and Normalization of each criterion"""
    tripleDash=0
    hyphens=0
    noC=0
    priceWithOther=0
    notWeighted=0
    ValueswithName=0
    totx=0
    manyLots=0
    critid=0
    cursor = database.cursor()
    request = "DROP TABLE IF EXISTS Criteria"
    sql = cursor.execute(request)
    request = "CREATE TABLE Criteria (criterionId INTEGER,lotId INTEGER,name TEXT,weight INTEGER,type TEXT,PRIMARY KEY(criterionId))"
    sql = cursor.execute(request)
    datas = pd.read_sql_query("SELECT * FROM CriteriaTemp", database,dtype=str) 
    datas["CRIT_PRICE_WEIGHT"] =datas["CRIT_PRICE_WEIGHT"].str.replace("-","",regex=True)
    datas["CRIT_PRICE_WEIGHT"] = datas["CRIT_PRICE_WEIGHT"].str.replace("[a-z]+","",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace("/","---",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace("---","&",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace("[a-z]+","",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace("%","",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace("; ","$",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace(";",".",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace(r" - ",r"$",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace("-","",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace(r" - ",r"$",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace(r"(\d\d) ?? (\d\d)",r"\1",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace("[??????-]","",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace("([\(\[]).*?([\)\]])", "",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace("^[0&]+","",regex=True)
    for i in range(len(datas)):
        prix = datas.loc[i,"CRIT_PRICE_WEIGHT"]
        weights = datas.loc[i,"CRIT_WEIGHTS"]
        criteria = datas.loc[i,"CRIT_CRITERIA"]
        if str(criteria).count('LOT')>2:
            manyLots=manyLots+1
        somme=0
        listecrit = []
        if len(str(prix))>0 and not(str(prix)=="nan"):
            temp = safe_cast(prix,float)
            if not(temp==None):
                totx=totx+temp
            if not(temp==None):
                listecrit.append(("PRIX",temp,"PRICE"))
                somme=temp+somme
        if len(str(weights))>0 and not(str(weights)=="nan"):
            critsW = str(weights).split("&")
            critsN = str(criteria).split("---")
            if len(critsN)>1:
                tripleDash = tripleDash+1
            for z in range(min(len(critsW),len(critsN))):
                temp = safe_cast(critsW[z],float)
                if not(temp==None):
                    totx = totx+temp
                else:
                    totx = totx+ 0
                if not(temp==None):
                    if temp==0.0:
                        temp = 1/min(len(critsW),len(critsN))
                    listecrit.append((critsN[z],temp,findType(critsN[z])))
                    if findType(critsN[z])=="PRICE":
                        priceWithOther=priceWithOther+1
                    somme=temp+somme
        elif len(str(criteria))>3:
            boolean = True
            temp = criteria.split("---")
            if len(temp)>1:
                tripleDash = tripleDash+1
            for l in range(len(temp)):
                nombres = re.findall("\d+", temp[l]) 
                critere = temp[l]
                if len(nombres)>0:
                    if boolean:
                        ValueswithName = ValueswithName+1
                        boolean=False
                    poids = float(nombres[0])
                    totx = totx+poids
                    if poids ==0:
                        poids = 1/len(temp)
                else:
                    poids= 1/len(temp)
                somme = somme+poids
                listecrit.append((critere,poids,findType(critere)))
                if findType(critsN[z])=="PRICE":
                        priceWithOther=priceWithOther+1
        if not(float(totx)==float(100)):
            notWeighted=notWeighted+1
        if len(listecrit)==1:
            if listecrit[0][2]=="MIXTE":
                cursor.execute("INSERT or IGNORE INTO Criteria values (?,?,?,?,?)",
                (critid,i,listecrit[0][0],50,"ENVIRONMENTAL"))
                critid = critid+1
                cursor.execute("INSERT or IGNORE INTO Criteria values (?,?,?,?,?)",
                (critid,i,listecrit[0][0],50,"SOCIAL"))
                critid = critid+1
            else:
                cursor.execute("INSERT or IGNORE INTO Criteria values (?,?,?,?,?)",
                (critid,i,listecrit[0][0],100,listecrit[0][2]))
                critid = critid+1
        elif len(listecrit)>1:
            for j in range(len(listecrit)):
                if listecrit[j][2]=="MIXTE":
                    cursor.execute("INSERT or IGNORE INTO Criteria values (?,?,?,?,?)",
                    (critid,i,listecrit[j][0],50*listecrit[j][1]/somme,"ENVIRONMENTAL"))
                    critid = critid+1
                    cursor.execute("INSERT or IGNORE INTO Criteria values (?,?,?,?,?)",
                    (critid,i,listecrit[j][0],50*listecrit[j][1]/somme,"SOCIAL"))
                    critid = critid+1
                else:
                    cursor.execute("INSERT or IGNORE INTO Criteria values (?,?,?,?,?)",
                    (critid,i,listecrit[j][0],100*listecrit[j][1]/somme,listecrit[j][2]))
                    critid = critid+1
    database.commit()
    return database



def siretization(database):
    """Siretization step"""
    cursor = database.cursor()
    datas = pd.read_sql_query("SELECT * FROM AgentsSiretiser", database,dtype=str) 
    request = "DROP TABLE IF EXISTS AgentsSiretiser"
    sql = cursor.execute(request)
    request = "CREATE TABLE IF NOT EXISTS AgentsSiretiser(agentId INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode TEXT,country TEXT, date TEXT,catJuridique TEXT,ids TEXT,type TEXT, PRIMARY KEY(agentId))"
    sql = cursor.execute(request)
    start = time.time()

    ###Creation of the databases
    cwd = os.getcwd()
    data_path = cwd+'/data/opening/*.csv'   ##Opening database
    d_p = cwd+'/data/Etab/*.csv'            # Every facilities in SIRENE
    d_s =cwd +'/data/foppaFiles/Sigles.csv' # Acronyms
    data_path_noms =cwd +'/data/foppaFiles/ChangementsNoms.csv' #Name modification during time

    bc = BlazingContext()
    col_types = ["str","str","str","str","str","str","str","str","str","str","str","str"]
    bc.create_table("etablissement",d_p,dtype=col_types)
    col_types = ["str","str"]
    bc.create_table('noms',data_path_noms,dtype=col_types)
    col_types = ["str","str","str"]
    bc.create_table('ouvertures',data_path,dtype=col_types)
    col_types = ["str","str"]
    bc.create_table('sigles',d_s,dtype=col_types)

    taille=len(datas)

    datas = datas.assign(siretPropose=0)
    datas = datas.assign(score=0)
    datas = datas.assign(NomSirene=0)
    datas = datas.assign(AdresseSirene=0)
    datas = datas.assign(VilleSirene=0)
    datas = datas.assign(CPSirene=0)


    dicoAssociation ={}
    ####Creation of specific datasets for CAEs

    ##CAF
    dfCAF = bc.sql("select * from etablissement WHERE TypeActivite = '84.30C' AND CatJuridique='8110'")
    dicoAssociation["8110"] = dfCAF
    ##CCI
    dfCCI= bc.sql("select * from etablissement WHERE TypeActivite = '94.11Z' AND CatJuridique='7381'")
    dicoAssociation["7381"] = dfCCI
    #dfSIVU
    dfSyndic= bc.sql("select * from etablissement WHERE CatJuridique='7353'")
    dicoAssociation["7353"] = dfSyndic
    #dfSyndicatMixte
    dfSyndicMixte= bc.sql("select * from etablissement WHERE CatJuridique='7354' OR CatJuridique='7355'")
    dicoAssociation["7354"] = dfSyndicMixte
    ##CCAS
    dfCCAS= bc.sql("select * from etablissement WHERE TypeActivite = '88.99B' AND CatJuridique='7361'")
    dicoAssociation["7361"] = dfCCAS
    ##Mairies
    dfMairie = bc.sql("select * from etablissement WHERE TypeActivite = '84.11Z' AND CatJuridique='7210'")
    dicoAssociation["7210"] = dfMairie
    ##Departement
    dfDepartement = bc.sql("select * from etablissement WHERE TypeActivite = '84.11Z' AND CatJuridique='7220'")
    dicoAssociation["7220"] = dfDepartement
    ##Region
    dfRegion = bc.sql("select * from etablissement WHERE TypeActivite = '84.11Z' AND CatJuridique='7230'")
    dicoAssociation["7230"] = dfRegion
    ##Communaut?? d'agglo
    dfComAgglo = bc.sql("select * from etablissement WHERE TypeActivite = '84.11Z' AND (CatJuridique='7348' OR CatJuridique='7344') ")
    dicoAssociation["7348"] = dfComAgglo
    dicoAssociation["7344"] = dfComAgglo
    ##Communaut?? de Communes
    dfComCom = bc.sql("select * from etablissement WHERE TypeActivite = '84.11Z' AND CatJuridique='7346'")
    dicoAssociation["7346"] = dfComCom
    ##Centre hospitalier
    dfChu = bc.sql("select * from etablissement WHERE TypeActivite = '86.10Z' AND CatJuridique='7364'")
    dicoAssociation["7364"] = dfChu
    end = time.time()
    for j in range(0,taille):
        ############## If the Siret is here, we replace the values immediatly:
        if len(str(datas["siret"][j]))>9:
            request = "SELECT nomUnite,num,typevoie,libelle,ville,cp FROM newtable WHERE siret = '"+str(datas["siret"][j])+ "'"
            gdf = bc.sql(request)
            if len(gdf)>0:
                datas["siretPropose"][j] = gdf['siret'][0]
                datas["score"][j] = maxi
                datas["NomSirene"][j] = gdf['nomUnite'][0]
                datas["AdresseSirene"][j] = str(gdf['num'][0])+" "+ str(gdf['typevoie'][0])+" "+ str(gdf['libelle'][0])
                datas["VilleSirene"][j] = gdf['ville'][0]
                datas["CPSirene"][j] = gdf['cp'][0]
            else: 
                print("Error : the Siret is incorrect. We will try to siretize")
                datas["siret"][j]=""
        else:
            start = time.time()
            gdf2 = pd.DataFrame()
            gdf3 = pd.DataFrame()
            gdf4 = pd.DataFrame()
            
            date = str(datas["date"][j])
            adresse = str(datas["address"][j])+ " "+str(datas["city"][j])
            nom = str(datas["name"][j])
            cat = str(int(float(datas["catJuridique"][j])))
            CP = str(datas["zipcode"][j])
            if len(CP)==4: ###Securite
                    CP = "0"+CP
            if len(CP)<5 or date=="nan": ##Sortie
                continue
            depart = CP[0:2]
            
            ######PREMIER FILTRAGE
            if cat in dicoAssociation:
                gdf = dicoAssociation[cat]
                bc.create_table("newtable",gdf)
                request = "SELECT nomEnseigne,nomUnite,siret,siren,num,typevoie,libelle,ville,CatJuridique,cp FROM newtable WHERE cp LIKE '"+ depart + "%'"
                gdf = bc.sql(request)
            else:
                request = "SELECT nomEnseigne,nomUnite,siret,siren,num,typevoie,libelle,ville,CatJuridique,cp FROM etablissement WHERE cp LIKE '"+ depart + "%' AND siret IN(select siret from ouvertures WHERE '"+date+"' BETWEEN date_debut AND date_fin)"
                gdf = bc.sql(request)
                nbCP = len(gdf)
            if len(gdf)>0:
                ### Jointure ACRONYMES
                bc.drop_table("newtable")
                bc.create_table("newtable",gdf)
                r = "SELECT * from newtable LEFT JOIN sigles ON newtable.siren = sigles.siren"
                gdf = bc.sql(r)
                
                ### Jointure ancienNoms
                bc.drop_table("newtable")
                bc.create_table("newtable",gdf)
                r = "SELECT * from newtable LEFT JOIN noms ON newtable.siren = noms.siren"
                gdf = bc.sql(r)
                
                ##
                bc.drop_table("newtable")
                bc.create_table("newtable",gdf)
                end = time.time()
                start=time.time()
                
                if len(gdf)>0:
                    vrainom = nom
                    noms=gdf["nomEnseigne"].to_arrow().to_pylist()
                    noms2=gdf["nomUnite"].to_arrow().to_pylist()
                    ancienNoms=gdf["Nom"].to_arrow().to_pylist()
                    sirets=gdf["siren"].to_arrow().to_pylist()
                    results = []
                    results2 = []
                    results3 = []
                    results4=[]
                    if cat in dicoAssociation:
                        sigles = []
                    else:
                        sigles=gdf["sigleUniteLegale"].to_arrow().to_pylist()
                        
                    if len(vrainom)<6:
                        results = process.extract(vrainom,noms,scorer=fuzz.token_set_ratio,score_cutoff=70,limit=500)
                        results2 = process.extract(vrainom,sigles,scorer=fuzz.token_set_ratio,score_cutoff=100,limit=100)
                        results3 = process.extract(vrainom,noms2,scorer=fuzz.token_set_ratio,score_cutoff=70,limit=100)
                        results4 = process.extract(vrainom,ancienNoms,scorer=fuzz.token_set_ratio,score_cutoff=70,limit=100)
                    else:
                        results = process.extract(vrainom,noms,scorer=fuzz.token_set_ratio,score_cutoff=60,limit=500)
                        results2 = process.extract(vrainom,sigles,scorer=fuzz.token_set_ratio,score_cutoff=100,limit=100)
                        results3 = process.extract(vrainom,noms2,scorer=fuzz.token_set_ratio,score_cutoff=70,limit=100)
                        results4 = process.extract(vrainom,ancienNoms,scorer=fuzz.token_set_ratio,score_cutoff=70,limit=100)
                    candidats = results+results2+results3+results4
                    end = time.time()
                    start=time.time()
                    gdf=gdf.fillna("")
                    temp = []
                    for i in range(len(candidats)):
                        temp.append(str(gdf["num"][candidats[i][2]])+" "+str(gdf["typevoie"][candidats[i][2]])+" "+str(gdf["libelle"][candidats[i][2]])+" "+str(gdf["ville"][candidats[i][2]]))
                    resultatsFinaux = process.extract(adresse,temp,scorer=fuzz.token_set_ratio,score_cutoff=0,limit=50)
                    scoreTotal = []
                    for num in range(len(resultatsFinaux)):
                        scoreTotal.append(int(resultatsFinaux[num][1])+int(candidats[resultatsFinaux[num][2]][1]))
                    scoreTotal = np.array(scoreTotal)
                    if len(scoreTotal)>0:
                        maxi = int(np.max(scoreTotal))
                        gagnant = np.argwhere(scoreTotal == np.amax(scoreTotal))
                        if len(gagnant)<2:
                            gagnant = int(gagnant[0])
                        else:
                            departage = []
                            for gagnantPossible in gagnant:
                                numgagnant = int(gagnantPossible)
                                position = int(candidats[int(resultatsFinaux[numgagnant][2])][2])
                                if pd.isna(gdf["nomEnseigne"][position]):
                                    gdf["nomEnseigne"][position] = ""
                                if pd.isna(gdf["nomUnite"][position]):
                                    gdf["nomUnite"][position] = ""
                                if pd.isna(gdf["sigleUniteLegale"][position]):
                                    gdf["sigleUniteLegale"][position] = ""
                                if pd.isna(gdf["Nom"][position]):
                                    gdf["Nom"][position] = ""
                                score = max(fuzz.ratio(vrainom,gdf["nomEnseigne"][position]),fuzz.ratio(vrainom,gdf["nomUnite"][position]),fuzz.ratio(vrainom,gdf["sigleUniteLegale"][position]),fuzz.ratio(vrainom,gdf["Nom"][position]))
                                departage.append(score)
                            gagnantDepartage = int(np.argmax(departage))
                            gagnant = int(gagnant[gagnantDepartage])
                        position = int(candidats[int(resultatsFinaux[gagnant][2])][2])
                    if len(candidats)>0:
                        datas["siretPropose"][j] = gdf['siret'][position]
                        datas["score"][j] = maxi
                        datas["NomSirene"][j] = gdf['nomUnite'][position]
                        datas["AdresseSirene"][j] = str(gdf['num'][position])+" "+ str(gdf['typevoie'][position])+" "+ str(gdf['libelle'][position])
                        datas["VilleSirene"][j] = gdf['ville'][position]
                        datas["CPSirene"][j] = gdf['cp'][position]
                    end = time.time()
                
    ids = np.array(datas["ids"]) 
    types = np.array(datas["type"]) 
    names = np.array(datas["name"]) 
    sirets = np.array(datas["siret"])
    newSirets = np.array(datas["siretPropose"])
    addresses = np.array(datas["address"]) 
    citys = np.array(datas["city"]) 
    zipcodes = np.array(datas["zipcode"]) 
    nomSirene = np.array(datas["NomSirene"])
    adresseSirene = np.array(datas["AdresseSirene"])
    villeSirene = np.array(datas["VilleSirene"])
    cpSirene = np.array(datas["CPSirene"])
    for i in range(len(sirets)):
        if len(str(newSirets[i]))>9:
            names[i] = nomSirene[i]
            addresses[i] = adresseSirene[i]
            citys[i] = villeSirene[i]
            zipcodes[i] = cpSirene[i]
    countrys = np.array(datas["country"]) 
    dates = np.array(datas["date"]) 
    catJuridique = np.array(datas["catJuridique"]) 

    for i in range(len(ids)):
        sql = ''' INSERT OR IGNORE INTO AgentsSiretiser(agentId,name,siret,address,city,zipcode,country,date,catJuridique,ids,type)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)'''
        val = (i,names[i],sirets[i],addresses[i],citys[i],zipcodes[i],countrys[i],dates[i],catJuridique[i],ids[i],types[i])
        cursor.execute(sql,val)
    database.commit()
    return database

def mergingAfterSiretization(database):
    datas1 = pd.read_sql_query("SELECT * FROM AgentsSiretiser WHERE siret is NULL", database,dtype=str) 
    datas2 = pd.read_sql_query("SELECT * FROM AgentsSiretiser WHERE siret is not NULL", database,dtype=str)
    datas2 = datas2.groupby(['siret'],as_index=False).agg({'ids':'-'.join,'name':'first','siret':'first','address':'first','city':'first','zipcode':'first','country':'first','date':'first','type':'first'}) 
    newDF = pd.concat([datas1,datas2])
    return newDF





def preProcess(column):
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    # If data is missing, indicate that by setting the value to `None`
    if not column:
        column = None
    return column


def readData(filename):
    row_id = 0
    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
            data_d[row_id] = dict(clean_row)
            row_id = row_id+1

    return data_d


def dedupeAgent(datas,database):
    datas.to_csv("ADeduper.csv",index=False)
    input_file = "ADeduper.csv"
    output_file = 'ResDedupe.csv'
    settings_file = 'data/SettingsDedupe'
    training_file = 'data/TrainingDedupe.json'

    print('importing data ...')
    data_d = readData(input_file)

    # If a settings file already exists, we'll just load that and skip training
    if os.path.exists(settings_file):
        print('reading from', settings_file)
        with open(settings_file, 'rb') as f:
            deduper = dedupe.StaticDedupe(f)
    else:
        # ## Training

        # Define the fields dedupe will pay attention to
        fields = [
            {'field': 'name', 'type': 'String'},
            {'field': 'address', 'type': 'String', 'has missing': True},
            {'field': 'city', 'type': 'String', 'has missing': True},
            {'field': 'zipcode', 'type': 'String', 'has missing': True},
            ]

        # Create a new deduper object and pass our data model to it.
        deduper = dedupe.Dedupe(fields)

        # If we have training data saved from a previous run of dedupe,
        # look for it and load it in.
        # __Note:__ if you want to train from scratch, delete the training_file
        if os.path.exists(training_file):
            print('reading labeled examples from ', training_file)
            with open(training_file, 'rb') as f:
                deduper.prepare_training(data_d, f)
        else:
            deduper.prepare_training(data_d)

        # ## Active learning
        # Dedupe will find the next pair of records
        # it is least certain about and ask you to label them as duplicates
        # or not.
        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        print('starting active labeling...')

        dedupe.console_label(deduper)

        # Using the examples we just labeled, train the deduper and learn
        # blocking predicates
        deduper.train()

        # When finished, save our training to disk
        with open(training_file, 'w') as tf:
            deduper.write_training(tf)

        # Save our weights and predicates to disk.  If the settings file
        # exists, we will skip all the training and learning next time we run
        # this file.
        with open(settings_file, 'wb') as sf:
            deduper.write_settings(sf)

    # ## Clustering

    # `partition` will return sets of records that dedupe
    # believes are all referring to the same entity.

    print('clustering...')
    clustered_dupes = deduper.partition(data_d, 0.75)

    print('# duplicate sets', len(clustered_dupes))

    # ## Writing Resultsphot

    # Write our original data back out to a CSV with a new column called
    # 'Cluster ID' which indicates which records refer to each other.

    cluster_membership = {}
    for cluster_id, (records, scores) in enumerate(clustered_dupes):
        for record_id, score in zip(records, scores):
            cluster_membership[record_id] = {
                "Cluster ID": cluster_id,
                "confidence_score": score
            }

    with open(output_file, 'w',newline='') as f_output, open(input_file) as f_input:

        reader = csv.DictReader(f_input)
        fieldnames = ['Cluster ID', 'confidence_score'] + reader.fieldnames
        writer = csv.DictWriter(f_output, fieldnames=fieldnames)
        writer.writeheader()
        compt=0
        for row in reader:
            row.update(cluster_membership[compt])
            compt = compt+1
            writer.writerow(row)
    
    
    return database
    
def finalTableAgent(database):
    cursor = database.cursor()
    clients = pd.read_sql_query("SELECT * FROM LotClients", database,dtype=str)
    suppliers = pd.read_sql_query("SELECT * FROM LotSuppliers", database,dtype=str)
    names = pd.read_sql_query("SELECT * FROM Names", database,dtype=str)
    datas = pd.read_csv("ResDedupe.csv",dtype=str,sep=",")
    lenCluster = len(datas.groupby("Cluster ID").count())
    request = "DROP TABLE IF EXISTS Agents"
    sql = cursor.execute(request)
    request = "CREATE TABLE Agents(agentId INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode	TEXT,country TEXT, department TEXT,longitude TEXT, latitude TEXT,PRIMARY KEY(agentId))"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS LotClients"
    sql = cursor.execute(request)
    request = "CREATE TABLE LotClients(lotId INTEGER,agentId INTEGER)"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS LotSuppliers"
    sql = cursor.execute(request)
    request = "CREATE TABLE LotSuppliers(lotId INTEGER,agentId INTEGER)"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS Names"
    sql = cursor.execute(request)
    request = "CREATE TABLE Names(agentId INTEGER,name TEXT,PRIMARY KEY(agentId,name))"
    sql = cursor.execute(request)

    dico = {}
    ClusterIds = np.array(datas["Cluster ID"])
    agentsID = np.array(datas["ids"])
    for j in range(len(agentsID)):
        temp = str(agentsID[j]).split("-")
        for k in range(len(temp)):
            dico[int(temp[k])] = int(ClusterIds[j])
            
    clientsLot = np.array(clients["lotId"])
    clientsAgent = np.array(clients["agentId"])
    suppliersLot = np.array(suppliers["lotId"])
    suppliersAgent = np.array(suppliers["agentId"])
    namesID = np.array(names["agentId"])
    namesAgent = np.array(names["name"]) 

    for i in range(len(clientsLot)):
        if (int(clientsAgent[i]) in dico):
            sql = ''' INSERT OR IGNORE INTO LotClients(lotId,agentId)
                        VALUES (?,?)'''
            val = (int(clientsLot[i]),int(dico[int(clientsAgent[i])]))
            cursor.execute(sql,val)
            
    for i in range(len(suppliersLot)):
        if (int(suppliersAgent[i]) in dico):
            sql = ''' INSERT OR IGNORE INTO LotSuppliers(lotId,agentId)
                        VALUES (?,?)'''
            val = (int(suppliersLot[i]),int(dico[int(suppliersAgent[i])]))
            cursor.execute(sql,val)
    
    for j in range(lenCluster):   
        numero = str(j)
        temp = datas[datas["Cluster ID"]==numero]
        temp = temp.reset_index()
        ##Selection of the Agent. 
        if len(temp)==1:
            sql = ''' INSERT OR IGNORE INTO Agents(agentId,name,siret,address,city,zipcode,country,department,longitude,latitude)
                        VALUES (?,?,?,?,?,?,?,?,?,?)'''
            val = (j,temp["name"][0],temp["siret"][0],temp["address"][0],temp["city"][0],temp["zipcode"][0],temp["country"][0],0,None,None)
            cursor.execute(sql,val)
        else:
            maxID = 0
            maxScore=0
            for candidat in range(len(temp)):
                score = len(str(temp["ids"][candidat]).split('-'))+1000*math.floor(len(str(temp["siret"][candidat]))/12)
                if score>maxScore:
                    maxScore=score
                    maxID=candidat
            sql = ''' INSERT OR IGNORE INTO Agents(agentId,name,siret,address,city,zipcode,country,department,longitude,latitude)
                        VALUES (?,?,?,?,?,?,?,?,?,?)'''
            val = (j,temp["name"][candidat],temp["siret"][candidat],temp["address"][candidat],temp["city"][candidat],temp["zipcode"][candidat],temp["country"][candidat],0,None,None)
            cursor.execute(sql,val)  
    for i in range(len(namesID)):
        if (int(namesID[i]) in dico):
            sql = ''' INSERT OR IGNORE INTO Names(agentId,name)
                        VALUES (?,?)'''
            val = (dico[int(namesID[i])],namesAgent[i])
            cursor.execute(sql,val)

    
    request = "UPDATE Agents SET name = NULLIF(name,'None')"
    sql = cursor.execute(request)
    request = "UPDATE Agents SET siret = NULLIF(siret,'None')"
    sql = cursor.execute(request)
    request = "UPDATE Agents SET address = NULLIF(address,'None')"
    sql = cursor.execute(request)
    request = "UPDATE Agents SET city = NULLIF(city,'None')"
    sql = cursor.execute(request)
    request = "UPDATE Agents SET zipcode = NULLIF(zipcode,'None')"
    sql = cursor.execute(request)
    database.commit()
    return database

def addSireneInfo(database):
    """ Adding depatment + additional information from SIRENE for siretized Agents (latitude,longitude)"""
    cursor = database.cursor()
    
    #Load GeoSirene
    filename = "data/geolocate/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.csv"
    chunksize = 10 ** 6
    ds = []
    for chunk in pd.read_csv(filename, chunksize=chunksize,dtype = str,sep=";"):
        chunk = chunk[["siret","x_longitude","y_latitude"]]
        ds.append(chunk)
    
    geoSirene = pd.concat(ds) 
    del ds   
    
    #Put geoSirene in the database
    geoSirene.to_sql('geolocate',database, if_exists='append', index = False)
    
    # Agents from the final step
    agents = pd.read_sql_query("SELECT * FROM Agents", database,dtype=str)
    request = "DROP TABLE IF EXISTS Agents"
    sql = cursor.execute(request)
    request = "CREATE TABLE Agents(agentId INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode	TEXT,country TEXT, department TEXT,longitude TEXT, latitude TEXT,PRIMARY KEY(agentId))"
    
    
    sql = cursor.execute(request)
    names = np.array(agents["name"])
    sirets = np.array(agents["siret"])
    addresses = np.array(agents["address"])
    citys = np.array(agents["city"])
    countrys = np.array(agents["country"])
    zipcodes = np.array(agents["zipcode"])
    dicoDepartement = {
        "200":"2A",
        "201":"2A",
        "202":"2B",
        "206":"2B",
        "971":"971",
        "972":"972",
        "973":"973",
        "974":"974",
        "975":"975",
        "976":"976",
        "977":"977",
        "978":"978",
        "986":"986",
        "987":"987",
        "988":"988"}    
    for i in range(len(agents)):
        
        ### Base Values
        long = None
        lat = None
        departement = None
        
        ### if siret : find lat and long
        if len(str(sirets[i]))>13:
            request = "SELECT x_longitude,y_latitude from geolocate where siret = '"+str(sirets[i])+"'"
            cursor.execute(request)
            results = cursor.fetchall()
            if len(results)>0:
                long = results[0][0]
                lat = results[0][1]
                
            
        ### departement matching:
        if len(str(zipcodes[i]))>4:
            departement = dicoDepartement.get(zipcodes[i][0:3],zipcodes[i][0:2])
        sql = ''' INSERT OR IGNORE INTO Agents(agentId,name,siret,address,city,zipcode,country,department,longitude,latitude)
                VALUES (?,?,?,?,?,?,?,?,?,?)'''
        val = (i,names[i],sirets[i],addresses[i],citys[i],zipcodes[i],countrys[i],departement,long,lat)
        cursor.execute(sql,val)
    ###
    
    request = "DROP TABLE IF EXISTS geolocate"
    cursor.execute(request)
    ###  
    database.commit()
    return database

def cleaningDatabase(database):
    """Cleaning of the temp tables and files"""
    cursor = database.cursor()
    request = "DROP TABLE IF EXISTS CriteriaTemp"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS AgentsTemp"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS AgentsSiretiser"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS AgentsLink"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS AgentsBase"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS CriteriaTemp"
    sql = cursor.execute(request)
    database.commit()
    os.remove("ADeduper.csv")
    os.remove("ResDedupe.csv")
    os.remove("data/geolocate/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.csv")
    return database


def contractNoticesCompletion(database):
    cursor = database.cursor()
    listeFichiers = []
    newDF = []
    for (repertoire, sousRepertoires, fichiers) in walk("data/contractNotices/"):
        listeFichiers.extend(fichiers)
    for j in listeFichiers:
        datas = pd.read_csv("data/contractNotices/"+j,dtype=str)
        datas = datas[datas["ISO_COUNTRY_CODE"].str.contains("FR")]
        newDF.append(datas[["ID_NOTICE_CN","FUTURE_CAN_ID","B_RENEWALS","DURATION","DT_DISPATCH","DT_APPLICATIONS"]])
    result = pd.concat(newDF)
    del newDF
    result.assign(pubDur=0)
    debut = np.array(result["DT_DISPATCH"])
    fin = np.array(result["DT_APPLICATIONS"])
    temp = len(debut)*[np.NAN]
    dictionnaireMois= {"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06","JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}
    for i in range(len(debut)):
        if len(str(debut[i]))>4 and len(str(fin[i]))>4:
            d = debut[i].split("-")
            f = fin[i].split("-")
            newd = d[2]+"-"+dictionnaireMois[d[1]]+"-"+d[0]
            newf = f[2]+"-"+dictionnaireMois[f[1]]+"-"+f[0]
            date1 = date(int(d[2]),int(dictionnaireMois[d[1]]),int(d[0]))
            date2 = date(int(f[2]),int(dictionnaireMois[f[1]]),int(f[0]))
            temp[i]=(date2-date1).days
    result["pubDur"] = temp
    result.to_sql('contractNotice',database, if_exists='append', index = False)
    
    ####AddingLotsInfo
    lots = pd.read_sql_query("SELECT * FROM Lots", database,dtype=str)
    for i in range(len(lots)):
        renewals= None
        contractDuration = None
        publicityDuration = None
        tedCanId = lots["tedCanId"][i]
        lotId = lots["lotId"][i]
        request = "SELECT B_RENEWALS,DURATION,pubDur from contractNotice where FUTURE_CAN_ID = '"+str(tedCanId)+"'"
        cursor.execute(request)
        results = cursor.fetchall()
        if len(results)>0:
            renewals = results[0][0]
            contractDuration = results[0][1]
            publicityDuration = results[0][2]
        if not(renewals==None):
            request = "UPDATE Lots set renewal ='"+str(renewals)+"' WHERE lotId ='"+str(lotId)+"'"
            cursor.execute(request)
        if not(contractDuration==None):
            request = "UPDATE Lots set contractDuration ='"+str(contractDuration)+"' WHERE lotId ='"+str(lotId)+"'"
            cursor.execute(request)
        if not(publicityDuration==None):
            request = "UPDATE Lots set publicityDuration ='"+str(publicityDuration)+"' WHERE lotId ='"+str(lotId)+"'"
            cursor.execute(request)
    request = "DROP TABLE IF EXISTS contractNotices"
    sql = cursor.execute(request)
    database.commit()
    return database
    
    
def exportDatabase(database):
    os.mkdir("FOPPA")
    os.mkdir("FOPPA/csv")
    lots = pd.read_sql_query("SELECT * FROM Lots", database)
    lots.to_csv("FOPPA/csv/Lots.csv",index=False)
    agents = pd.read_sql_query("SELECT * FROM Agents", database)
    agents.to_csv("FOPPA/csv/Agents.csv",index=False)
    criteria = pd.read_sql_query("SELECT * FROM Criteria", database)
    criteria.to_csv("FOPPA/csv/Criteria.csv",index=False)
    lotsClients = pd.read_sql_query("SELECT * FROM LotClients", database)
    lotsClients.to_csv("FOPPA/csv/LotClients.csv",index=False)
    lotsSuppliers = pd.read_sql_query("SELECT * FROM LotSuppliers", database)
    lotsSuppliers.to_csv("FOPPA/csv/LotSuppliers.csv",index=False)
    names = pd.read_sql_query("SELECT * FROM Names", database)
    names.to_csv("FOPPA/csv/Names.csv",index=False)
    
    file = open("FOPPA/FOPPA.sql","w")
    for line in database.iterdump():
        print(line)
        file.write(line)
        file.write("\n")
        
##### Main 
if __name__ == '__main__':
    
    # Download files
    print("---Download---")
    downloadFiles()

    # Creation of the database
    print("---Creation of FOPPA---")
    db = databaseCreation("Foppa.db")
    
    # Load each csv files of data europa
    print("---Load TED Files---")
    datas = load_csv_files()

    # First cleaning and filling of the database
    print("---ProcessingPart1---")
    db = firstCleaning(datas,db)

    # Second processing
    print("---ProcessingPart2---")
    db = mainCleaning(db)

    #Normalization
    print("---ProcessingPart3---")
    db = fineTuningAgents(db)

    # Criteria processing 
    print("---Criteria---")
    db = criteriaProcessing(db)

    # Siretization step
    db = siretization(db)

    # Update of the database according to the siretization
    print("---Merging---")
    datas = mergingAfterSiretization(db)

    # Dedupe step
    print("---Deduping---")
    db = dedupeAgent(datas,db)

    # Update of the database according to Dedupe
    print("---Final Agent---")
    db = finalTableAgent(db)
    
    #Complete with SIRENE additional information 
    print("---SIRENE Info---")
    db = addSireneInfo(db)
    
    # Complete with CN additional information
    print("---CN Info---")
    db = contractNoticesCompletion(db)
    
    # Delete temporary files
    print("---Clean---")
    db = cleaningDatabase(db)
    
    # Export the database
    print("---Export---") 
    exportDatabase(db)
    db.close()
    #os.remove("Foppa.db")
    del db
    