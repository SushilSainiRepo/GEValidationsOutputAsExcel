import pandas as pd
import math as m
import os as os

ConfigfilePath = f"DynamicValidation/metadata/ValidationsConfig.xlsx"
print(os.name)
files = pd.read_excel(ConfigfilePath, sheet_name="FilesList")
Accounts = pd.read_excel(ConfigfilePath, sheet_name="Accounts")
Columns = pd.read_excel(ConfigfilePath, sheet_name="Columns")
Countries = pd.read_excel(ConfigfilePath, sheet_name="CountryCode")
Warehouses = pd.read_excel(ConfigfilePath, sheet_name="Warehouse")


for id, filedetail in files.iterrows():
    
    fileIdentifier = filedetail["fileidentifier"]
    filename = filedetail["filename"]
    # if fileIdentifier =="P14060_Expired_Gift_Vouchers" :
    print("-------------------", chr(10), chr(13))
    print("fileidentity =", fileIdentifier, "filename =", filename)
    # else:
    #     continue

    ColumnsForFile = Columns[Columns["fileidentifier"] == fileIdentifier]
    
    print(chr(10), chr(13))
    if ColumnsForFile.count()[0] > 0:
        print("Columns list", ColumnsForFile["column"].tolist())
        UniqueColumnsForFile = ColumnsForFile[ColumnsForFile["unique"] == 1]
        if UniqueColumnsForFile.count()[0] > 0:
            print(chr(10), chr(13))
            print("Unique Columns", UniqueColumnsForFile["column"].tolist())
            print(chr(10), chr(13))
        else:
            print(chr(10), chr(13))
            print("Unique Columns = ", "NA")
            print(chr(10), chr(13))
    AccountsForFile = Accounts[Accounts["fileidentifier"] == fileIdentifier]
    if AccountsForFile.count()[0] > 0:
        print("Accounts =", AccountsForFile["accountcode"].tolist())
    else:
        print("Accounts = NA")
    Periods = pd.read_excel(ConfigfilePath, sheet_name="Periods")
    PeriodsForFile = Periods[Periods["fileidentifier"] == fileIdentifier]
    startPeriod = 0
    EndPeriod = 0
    if PeriodsForFile.count()[0] > 0:
        startPeriod = PeriodsForFile["startperiod"].values[0]
        EndPeriod = PeriodsForFile["endperiod"].values[0]
        CheckPeriods = []
        missingPeriods=set()
        if type(PeriodsForFile["missedperiods"].values[0]) == str :
            tempArray= PeriodsForFile["missedperiods"].values[0].replace(' ','').split(",")
            ListOfMissingPeriods = [int(numeric_string) for numeric_string in tempArray ]
            missingPeriods = set(sorted(ListOfMissingPeriods))
        

        print (missingPeriods)
        for x in range(2018,2024):
            for y in range(1, 13):
                if x * 100 + y >= startPeriod and x * 100 + y <= EndPeriod:
                    period= set([x*100+y])
                    if missingPeriods.issuperset( period)==False : 
                        CheckPeriods.append(x * 100 + y)
                    else : print (period , " is to be excluded ")
        print(chr(10), chr(13))
        print("Periods = ", CheckPeriods)
    else:
        print("Periods = NA")

    print(chr(10), chr(13))
