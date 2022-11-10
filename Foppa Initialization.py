## Foppa Initialization


import json
import sqlite3
import pandas as pd
import numpy as np 
import os
import re
import time
from os import walk
from blazingsql import BlazingContext
from rapidfuzz import fuzz
from rapidfuzz import process

def databaseCreation(nameDatabase):
    """Creation of the tables of the database"""
    database = sqlite3.connect(nameDatabase)
    cursor = database.cursor()
    request = "DROP TABLE IF EXISTS AgentsBase"
    sql = cursor.execute(request)
    request = "CREATE TABLE AgentsBase(agentID INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode	TEXT,country TEXT, date TEXT,type TEXT,PRIMARY KEY(agentID))"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS Agents"
    sql = cursor.execute(request)
    request = "CREATE TABLE Agents(agentID INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode	TEXT,country TEXT, department TEXT,PRIMARY KEY(agentID))"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS AgentsLink"
    sql = cursor.execute(request)
    request = "CREATE TABLE AgentsLink(temporalagentID INTEGER,agentID INTEGER)"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS CriteriaTemp"
    sql = cursor.execute(request)
    request = "CREATE TABLE CriteriaTemp (CRIT_PRICE_WEIGHT TEXT,CRIT_WEIGHTS TEXT, CRIT_CRITERIA TEXT)"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS Lots"
    sql = cursor.execute(request)
    request = "CREATE TABLE Lots(lotID INTEGER,tedCANID INTEGER,corrections INTEGER,cancelled INTEGER,awardDate TEXT,awardEstimatedPrice NUMERIC,awardPrice NUMERIC,CPV TEXT,tenderNumber INTEGER,onBehalf TINYINT,jointProcurement TINYINT,fraAgreement TINYINT,fraEstimated INTEGER,lotsNumber INTEGER,accelerated TINYINT,outOfDirectives TINYINT,contractorSME TINYINT,numberTendersSME INTEGER,subContracted TINYINT,gpa	TINYINT,multipleCAE	TINYINT,typeOfContract TEXT,topType	TEXT,PRIMARY KEY(lotID))"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS LotClients"
    sql = cursor.execute(request)
    request = "CREATE TABLE LotClients(lotID INTEGER,agentID INTEGER,FOREIGN KEY(agentID) REFERENCES Agents(agentID) ON UPDATE CASCADE,FOREIGN KEY(lotID) REFERENCES Lots(lotID) ON UPDATE CASCADE)"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS LotSuppliers"
    sql = cursor.execute(request)
    request = "CREATE TABLE LotSuppliers(lotID INTEGER,agentID INTEGER,FOREIGN KEY(agentID) REFERENCES Agents(agentID) ON UPDATE CASCADE,FOREIGN KEY(lotID) REFERENCES Lots(lotID) ON UPDATE CASCADE)"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS Names"
    sql = cursor.execute(request)
    request = "CREATE TABLE Names(agentID INTEGER,name TEXT)"
    sql = cursor.execute(request)
    database.commit()
    return database

def load_csv_files():
    """Extraction of the csv files into the database"""
    listeFichiers = []
    newDF = []
    for (repertoire, sousRepertoires, fichiers) in walk("../data/TedAwardNotices/csv"):
        listeFichiers.extend(fichiers)
    for j in listeFichiers:
        datas = pd.read_csv("../data/TedAwardNotices/csv/"+j,dtype=str)
        # We only keep french contracts
        newDF.append(datas[datas["ISO_COUNTRY_CODE"].str.contains("FR")])
    result = pd.concat(newDF)
    return result

def firstCleaning(datas,database):
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
    tedCANID = np.array(datas["ID_NOTICE_CAN"])
    corrections = np.array(datas["CORRECTIONS"])
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
    contractorSME = np.array(datas["B_CONTRACTOR_SME"])
    numbersTendersSME = np.array(datas["NUMBER_TENDERS_SME"])
    subContracted = np.array(datas["B_SUBCONTRACTED"])
    gpa = np.array(datas["B_GPA"])
    multipleCAE = np.array(datas["B_MULTIPLE_CAE"])
    typeofContract = np.array(datas["TYPE_OF_CONTRACT"])
    topType = np.array(datas["TOP_TYPE"])

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
        sql = ''' INSERT INTO Lots(lotID,tedCANID,corrections,cancelled,awardDate,awardEstimatedPrice,awardPrice,CPV,tenderNumber,onBehalf,jointProcurement,fraAgreement,fraEstimated,lotsNumber,accelerated,outOfDirectives,contractorSME,numberTendersSME,subContracted,gpa,multipleCAE,typeOfContract,topType)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
        val = (i,tedCANID[i],corrections[i],cancelled[i],awardDate[i],awardEstimatedPrice[i],awardPrice[i],cpv[i],tenderNumber[i],onBehalf[i],jointProcurement[i],fraAgreement[i],fraEstimated[i],lotsNumber[i],accelerated[i],outOfDirectives[i],contractorSME[i],numbersTendersSME[i],subContracted[i],gpa[i],multipleCAE[i],typeofContract[i],topType[i])
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
            sql = ''' INSERT INTO AgentsBase(agentID,name,siret,address,city,zipcode,country,date,type)
                VALUES (?,?,?,?,?,?,?,?,?)'''
            val = (compteurAgent,tempName,tempSiret,tempAddress,tempTown,tempPC,tempCountry,date,"CAE")
            cur.execute(sql,val)
            sql = ''' INSERT INTO LotClients(lotID,agentID)
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
            sql = ''' INSERT INTO AgentsBase(agentID,name,siret,address,city,zipcode,country,date,type)
                    VALUES (?,?,?,?,?,?,?,?,?)'''
            val = (compteurAgent,tempName,tempSiret,tempAddress,tempTown,tempPC,tempCountry,date,"WIN")
            cur.execute(sql,val)
            sql = ''' INSERT INTO LotSuppliers(lotID,agentID)
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
    datas2 = datas.groupby(["name",'address','city','siret'],as_index=False).agg({'agentID':'-'.join,'name':'first','siret':'first','address':'first','city':'first','zipcode':'first','country':'first','date':'first','type':'first'})
    datas = datas2
    cursor = database.cursor()
    request = "DROP TABLE IF EXISTS AgentsTemp"
    sql = cursor.execute(request)
    request = "CREATE TABLE IF NOT EXISTS AgentsTemp(agentID INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode	TEXT,country TEXT, date TEXT,ids TEXT,type TEXT,PRIMARY KEY(agentID))"
    sql = cursor.execute(request)

    types = np.array(datas["type"]) 
    ids = np.array(datas["agentID"]) 
    names = np.array(datas["name"]) 
    sirets = np.array(datas["siret"]) 
    addresses = np.array(datas["address"]) 
    citys = np.array(datas["city"]) 
    zipcodes = np.array(datas["zipcode"]) 
    countrys = np.array(datas["country"]) 
    dates = np.array(datas["date"]) 

    for i in range(len(ids)):
        sql = ''' INSERT INTO AgentsTemp(agentID,name,siret,address,city,zipcode,country,date,ids,type)
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
    request = "CREATE TABLE IF NOT EXISTS AgentsSiretiser(agentID INTEGER,name TEXT,siret TEXT,address TEXT,newAddress TEXT,city TEXT,zipcode TEXT,country TEXT, date TEXT,catJuridique TEXT,company TEXT,ids TEXT,type TEXT, PRIMARY KEY(agentID))"
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
    hexaposte = pd.read_csv("../data/hexaPoste/hexaposte.csv",dtype=str,sep=";")
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
        if not(country[i] == "nan" or country[i] == "FR" or country[i] == "PF" or country[i] == "WF" or country[i] == "NC" or country[i] == "YT" or country[i] == "RE" or country[i] == "GF" or country[i] == "MQ" or country[i] == "GP"):
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
    convertisseur = pd.read_csv("../data/foppaFiles/CatJuridique.csv",dtype=str,sep=";")
    datas = datas.assign(catJuridique=None)
    for i in range(len(convertisseur)):
        temp = convertisseur["Nom"][i]
        datas.loc[datas["name"].str.contains(temp,regex=True,na=False),'catJuridique' ] = convertisseur["catJuridique"][i]
    import copy as cp
        #### Adresses
    datasetEntites=cp.deepcopy(datas)
    libellesConvert = pd.read_csv("Libelle.csv",dtype=str)
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
    print(datasetExtraction)
    datasetEntites["CodeVoie"] = datasetExtraction[0]
    datasetEntites["LibelVoie"] = datasetExtraction[1]


    ##Regex num+type+libelle
    datasetEntites["NewAdress"] = datasetEntites["address"]
    reg="([0-9]+) (" +listeLibel + ") ([A-Z,0-9, ]+)("+endingschar+")"
    datasetEntites["NewAdress"] = datasetEntites["address"]
    datasetExtraction = datasetEntites["NewAdress"].str.extract(reg)
    datasetEntites2 = datasetEntites.copy()
    print(datasetExtraction[0])
    datasetEntites2["NumVoie"] = datasetExtraction[0]
    print(datasetEntites2["NumVoie"])
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
        sql = ''' INSERT INTO AgentsSiretiser(agentID,name,siret,address,newAddress,city,zipcode,country,date,catJuridique,ids,type)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''
        val = (i,names[i],sirets[i],addresses[i],Newaddresses[i],citys[i],zipcodes[i],countrys[i],dates[i],catJuridique[i],ids[i],types[i])
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
    database.commit()
    return database



def safe_cast(val, to_type, default=None):
    try:
        return to_type(val)
    except (ValueError, TypeError):
        return default
    
def findType(chaine):
    lchaine = str.upper(chaine)
    if "TECHNIQUE" in lchaine:
        return("TECHNIQUE")
    if "TEHNIQUE" in lchaine:
        return("TECHNIQUE")
    if "PRIX" in lchaine:
        return("PRIX")
    if "DELAI" in lchaine:
        return("DELAI")
    if "ENVIRONNEMENT" in lchaine:
        return("ENVIRONNEMENT")
    if "REMISE" in lchaine:
        return("PRIX")
    if "MONTANT" in lchaine:
        return("PRIX")
    if "ECONOMIQUE" in lchaine:
        return("PRIX")
    if "DURABLE" in lchaine:
        return("ENVIRONNEMENT")
    if "COUT" in lchaine:
        return("PRIX") 
    if "TARIF" in lchaine:
        return("PRIX")
    if "FINANCIER" in lchaine:
        return("PRIX")
    if "SOCIAL" in lchaine:
        return("SOCIAL")
    return("AUTRES")


def criteriaProcessing(database):
    cursor = database.cursor()
    request = "DROP TABLE IF EXISTS Criteria"
    sql = cursor.execute(request)
    request = "CREATE TABLE Criteria (critereID INTEGER,lotID INTEGER,name TEXT,weight INTEGER,type TEXT,Total INTEGER,PRIMARY KEY(critereID),FOREIGN KEY(lotID) REFERENCES Lots(lotID) ON UPDATE CASCADE)"
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
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace(r"(\d\d) à (\d\d)",r"\1",regex=True)
    datas["CRIT_WEIGHTS"] = datas["CRIT_WEIGHTS"].str.replace("[éùà-]","",regex=True)
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
                listecrit.append(("PRIX",temp,"PRIX"))
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
                    if findType(critsN[z])=="PRIX":
                        priceWithOther=priceWithOther+1
                    somme=temp+somme
        elif len(str(criteria))>3:
            boolean = True
            temp = criteria.split("---")
            if len(temp)>1:
                tripleDash = tripleDash+1
            for l in range(len(temp)):
                nombres = re.findall("\d+", temp[l]) 
                #print(nombres)
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
                if findType(critsN[z])=="PRIX":
                        priceWithOther=priceWithOther+1
        if not(float(totx)==float(100)):
            notWeighted=notWeighted+1
        if len(listecrit)==1:
            cursor.execute("INSERT or IGNORE INTO Criteria values (?,?,?,?,?,?)",
            (critid,i,listecrit[0][0],100,listecrit[0][2],totx))
        elif len(listecrit)>1:
            for j in range(len(listecrit)):
                cursor.execute("INSERT or IGNORE INTO Criteria values (?,?,?,?,?,?)",
                (critid,i,listecrit[j][0],100*listecrit[j][1]/somme,listecrit[j][2],totx))
                critid = critid+1
    cursor.execute("DROP TABLE CriteriaTemp")
    database.commit()
    return database



def siretization(database):
    cursor = database.cursor()
    datas = pd.read_sql_query("SELECT * FROM AgentsSiretiser", database,dtype=str) 
    request = "DROP TABLE IF EXISTS AgentsSiretiser"
    sql = cursor.execute(request)
    request = "CREATE TABLE IF NOT EXISTS AgentsSiretiser(agentID INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode TEXT,country TEXT, date TEXT,catJuridique TEXT,company TEXT,ids TEXT,type TEXT, PRIMARY KEY(agentID))"
    sql = cursor.execute(request)
    start = time.time()

    ###Creation des BDD : 
    cwd = os.getcwd()
    data_path = cwd+'/data/Ouvertures/*.csv'
    d_p = cwd+'/data/Etab/*.csv'
    d_s =cwd +'/data/foppaFiles/Sigles2.csv'
    data_path_noms =cwd +'/data/foppaFiles/ChangementsNoms2.csv'

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
    ####Creation des datasets Speciaux:

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
    ##Communauté d'agglo
    dfComAgglo = bc.sql("select * from etablissement WHERE TypeActivite = '84.11Z' AND (CatJuridique='7348' OR CatJuridique='7344') ")
    dicoAssociation["7348"] = dfComAgglo
    dicoAssociation["7344"] = dfComAgglo
    ##Communauté de Communes
    dfComCom = bc.sql("select * from etablissement WHERE TypeActivite = '84.11Z' AND CatJuridique='7346'")
    dicoAssociation["7346"] = dfComCom
    ##Centre hospitalier
    dfChu = bc.sql("select * from etablissement WHERE TypeActivite = '86.10Z' AND CatJuridique='7364'")
    dicoAssociation["7364"] = dfChu
    end = time.time()
    print("Temps Init")
    print(end-start)
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
                print("Filtrage1")
                print(end-start)
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
                    print("Filtrage2")
                    print(end-start)
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
                    print("Filtrage3")
                    print(end-start)
                
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
        sql = ''' INSERT INTO AgentsSiretiser(agentID,name,siret,address,city,zipcode,country,date,catJuridique,ids,type)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)'''
        val = (i,names[i],sirets[i],addresses[i],citys[i],zipcodes[i],countrys[i],dates[i],catJuridique[i],ids[i],types[i])
        cursor.execute(sql,val)
    database.commit()
    return database

def mergingAfterSiretization(database):
    request = "SELECT * FROM AgentsSiretiser WHERE siret is not NULL"
    #atas2 = datas.groupby(["name",'address','city','siret'],as_index=False).agg({'agentID':'-'.join,'name':'first','siret':'first','address':'first','city':'first','zipcode':'first','country':'first','date':'first','type':'first'})
    return database


def deduping(database):
    
    
    return database
    
def finalTableAgent(database):
    cursor = database.cursor()
    datas = pd.read_sql_query("SELECT * FROM AgentsSiretiser", database,dtype=str) 
    ids = np.array(datas["ids"]) 
    types = np.array(datas["type"]) 
    names = np.array(datas["name"]) 
    sirets = np.array(datas["siret"])
    addresses = np.array(datas["address"]) 
    citys = np.array(datas["city"]) 
    zipcodes = np.array(datas["zipcode"]) 
    countrys = np.array(datas["country"]) 
    
    ############
    #French special departement
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
    departement = []     
    for i in range(len(datas)):
        ### Modif departement:
        departement = dicoDepartement.get(zipcodes[i][0:3],zipcodes[i][0:2])
        sql = ''' INSERT INTO Agents(agentID,name,siret,address,city,zipcode,country,department)
                VALUES (?,?,?,?,?,?,?,?)'''
        val = (i,names[i],sirets[i],addresses[i],citys[i],zipcodes[i],countrys[i],departement)
        oldNumbers = str(ids[i]).split("-")
        for old in oldNumbers:
            request ="UPDATE LotClients SET agentID = '"+str(i)+"' WHERE agentID = '"+str(old)+"'"
            sql = cursor.execute(request)
            request ="UPDATE LotSuppliers SET agentID = '"+str(i)+"' WHERE agentID = '"+str(old)+"'"
            sql = cursor.execute(request)
    
    ###  
    database.commit()
    return database
    
def informationCompletion(database):
    return database
        

##### Main 
db = databaseCreation("FoppaTEST.db")
datas = load_csv_files()
db = firstCleaning(datas,db)
db = mainCleaning(db)
db = fineTuningAgents(db)
#db = criteriaProcessing(db)
#db = siretisationProcessing(db)

