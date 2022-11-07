## Foppa Initialization


import json
import sqlite3
import pandas as pd
import numpy as np 
from os import walk

def databaseCreation(nameDatabase):
    database = sqlite3.connect(nameDatabase)
    cursor = database.cursor()
    request = "DROP TABLE IF EXISTS AgentsBase"
    sql = cursor.execute(request)
    request = "CREATE TABLE AgentsBase(agentID INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode	TEXT,country TEXT, date TEXT, PRIMARY KEY(agentID))"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS Agents"
    sql = cursor.execute(request)
    request = "CREATE TABLE Agents(agentID INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode	TEXT,country TEXT, department TEXT,PRIMARY KEY(agentID))"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS AgentsLink"
    sql = cursor.execute(request)
    request = "CREATE TABLE AgentsLink(temporalagentID INTEGER,agentID INTEGER)"
    sql = cursor.execute(request)
    request = "DROP TABLE IF EXISTS Criteria"
    sql = cursor.execute(request)
    request = "CREATE TABLE Criteria (critereID INTEGER,lotID INTEGER,name TEXT,weight INTEGER,type TEXT,Total INTEGER,PRIMARY KEY(critereID),FOREIGN KEY(lotID) REFERENCES Lots(lotID) ON UPDATE CASCADE)"
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
    listeFichiers = []
    newDF = []
    for (repertoire, sousRepertoires, fichiers) in walk("../data/TedAwardNotices/csv"):
        listeFichiers.extend(fichiers)
    for j in listeFichiers:
        datas = pd.read_csv("../data/TedAwardNotices/csv/"+j,dtype=str)
        newDF.append(datas[datas["ISO_COUNTRY_CODE"].str.contains("FR")])
    result = pd.concat(newDF)
    return result

def firstCleaning(datas,database):
    columns = datas.columns
    for column in columns:
        datas[column] = datas[column].str.upper()
        datas[column]= datas[column].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
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

    awardDate = np.array(datas["DT_AWARD"])
    dispatchDate = np.array(datas["DT_DISPATCH"])
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
            sql = ''' INSERT INTO AgentsBase(agentID,name,siret,address,city,zipcode,country,date)
                VALUES (?,?,?,?,?,?,?,?)'''
            val = (compteurAgent,tempName,tempSiret,tempAddress,tempTown,tempPC,tempCountry,date)
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
            sql = ''' INSERT INTO AgentsBase(agentID,name,siret,address,city,zipcode,country,date)
                    VALUES (?,?,?,?,?,?,?,?)'''
            val = (compteurAgent,tempName,tempSiret,tempAddress,tempTown,tempPC,tempCountry,date)
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
            sirets[i]=None
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
    datas2 = datas.groupby(["name",'address','city'],as_index=False).agg({'agentID':'-'.join,'name':'first','siret':'first','address':'first','city':'first','zipcode':'first','country':'first','date':'first'})
    datas2 = datas
    cursor = database.cursor()
    request = "DROP TABLE IF EXISTS AgentsTemp"
    sql = cursor.execute(request)
    request = "CREATE TABLE IF NOT EXISTS AgentsTemp(agentID INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode	TEXT,country TEXT, date TEXT,ids TEXT, PRIMARY KEY(agentID))"
    sql = cursor.execute(request)

    ids = np.array(datas["agentID"]) 
    names = np.array(datas["name"]) 
    sirets = np.array(datas["siret"]) 
    addresses = np.array(datas["address"]) 
    citys = np.array(datas["city"]) 
    zipcodes = np.array(datas["zipcode"]) 
    countrys = np.array(datas["country"]) 
    dates = np.array(datas["date"]) 

    for i in range(len(ids)):
        sql = ''' INSERT INTO AgentsTemp(agentID,name,siret,address,city,zipcode,country,date,ids)
                VALUES (?,?,?,?,?,?,?,?,?)'''
        val = (i,names[i],sirets[i],addresses[i],citys[i],zipcodes[i],countrys[i],dates[i],ids[i])
        cursor.execute(sql,val)
    database.commit()
    return database

def fineTuningAgents(database):
    datas = pd.read_sql_query("SELECT * FROM AgentsTemp", database,dtype=str) 
    cursor = database.cursor()
    ## La table a siretiser au final : 
    request = "DROP TABLE IF EXISTS AgentsSiretiser"
    sql = cursor.execute(request)
    request = "CREATE TABLE IF NOT EXISTS AgentsSiretiser(agentID INTEGER,name TEXT,siret TEXT,address TEXT,city TEXT,zipcode TEXT,country TEXT, date TEXT,catJuridique TEXT,company TEXT,ids TEXT, PRIMARY KEY(agentID))"
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
    print(hexaposte.columns)
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
    print(convertisseur)
    datas = datas.assign(catJuridique=None)
    for i in range(len(convertisseur)):
        temp = convertisseur["Nom"][i]
        datas.loc[datas["name"].str.contains(temp,regex=True,na=False),'catJuridique' ] = convertisseur["catJuridique"][i]

    datas = datas.replace("NULL_IDENTIFIER",np.nan)

    ids = np.array(datas["agentID"]) 
    names = np.array(datas["name"]) 
    sirets = np.array(datas["siret"]) 
    addresses = np.array(datas["address"]) 
    citys = np.array(datas["city"]) 
    zipcodes = np.array(datas["newCP"]) 
    countrys = np.array(datas["country"]) 
    dates = np.array(datas["date"]) 
    catJuridique = np.array(datas["catJuridique"]) 

    for i in range(len(ids)):
        sql = ''' INSERT INTO AgentsSiretiser(agentID,name,siret,address,city,zipcode,country,date,catJuridique,ids)
                VALUES (?,?,?,?,?,?,?,?,?,?)'''
        val = (i,names[i],sirets[i],addresses[i],citys[i],zipcodes[i],countrys[i],dates[i],catJuridique[i],ids[i])
        cursor.execute(sql,val)
    database.commit()
    return database




##### Main 
db = databaseCreation("FoppaTEST.db")
datas = load_csv_files()
db = firstCleaning(datas,db)
db = mainCleaning(db)
db = fineTuningAgents(db)
