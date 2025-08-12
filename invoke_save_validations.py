# class great_expectations.checkpoint.types.checkpoint_result.CheckpointResult(run_id: RunIdentifier, run_results: dict[ValidationResultIdentifier, dict[str, ExpectationSuiteValidationResult | dict | str]], checkpoint_config: CheckpointConfig, validation_result_url: Optional[str] = None, success: Optional[bool] = None)
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResultSchema
import Modules as md
from io import StringIO
import json as json
import pandas as pd

baseurl = "DynamicValidation/files/validationresults/"
mode = 'w' #'a'
header = True # False;
opendocs = False
# from types import SimpleNamespace
####### Validate expectation Suites to run check point

Validations = md.FileValidationsHandler()
Validations.createcheckpoint("DataQuality")
FilesMatrix = Validations.runcheckpoint(CheckPointName="DataQuality", opendocs=opendocs)



# write to csv list of files not found
dataframe1 = pd.DataFrame({"Filename": FilesMatrix[0]})
#dataframe.to_csv(f"{baseurl}FilesMatrixNotFound.csv", index=False, mode=mode, header=header)




# write to csv list of files which are successful in recent run
dataframe2 = pd.DataFrame({"Successful": FilesMatrix[1]})
# dataframe.to_csv(f"{baseurl}FilesSuccessful.csv", index=False, mode=mode, header=header)



# result = js.loads(json_data, object_hook=lambda d: SimpleNamespace(**d))
# result = js.loads(json_data, object_hook=lambda d: SimpleNamespace(**d))


print("No of file not found or empty :", len(FilesMatrix[0]))
print("No of file passed :", len(FilesMatrix[1]))
print("No of file failed :", len(FilesMatrix[2]))
####### print errors only
statistics = []
statistics.append([])
statistics.append([])
statistics.append([])
statistics.append([])

ResultMatrix = []
ResultMatrix.append([])
ResultMatrix.append([])
ResultMatrix.append([])
ResultMatrix.append([])
ResultMatrix.append([])
ResultMatrix.append([])
ResultMatrix.append([])
ResultMatrix.append([])
ResultMatrix.append([])
ResultMatrix.append([])
ResultMatrix.append([])
ResultMatrix.append([])

#Loop through each batch which checkpoint processed to assess checkpoint result per run, which is 1 per file
for i in range(len(FilesMatrix[3])):
    faileddata = FilesMatrix[3][i]
    json_data = StringIO(str(faileddata))
    data = json.load(json_data)
    #: dict[str, CheckpointResult] 
    results:CheckpointResult= data
    #Loop though all the expecations run in the Suite to over results and access various related information
    for key in results["run_results"]:
        for keyidentifier in results["run_results"][key]["validation_result"]:
            keycomponents = results["run_results"][key]["validation_result"][keyidentifier]
            if keyidentifier == "meta":
                statistics[2].append(keycomponents["validation_time"])
                statistics[3].append(keycomponents["expectation_suite_name"])

            if keyidentifier == "statistics":
                statistics[0].append(keycomponents["unsuccessful_expectations"])
                statistics[1].append(keycomponents["successful_expectations"])
            if keyidentifier == "results":
                for j in range(len(keycomponents)):
                    ResultMatrix[0].append(
                        results["run_results"][key]["validation_result"]["meta"][
                            "expectation_suite_name"
                        ]
                    )
                    ResultMatrix[1].append(FilesMatrix[4][i])
                    ResultMatrix[2].append(
                        results["run_results"][key]["validation_result"]["meta"][
                            "validation_time"
                        ]
                    )
                    #enable following if you only want to output failed validation /expectation 
                    # if keycomponents[i]["success"]==False :
                    outcome = keycomponents[j]["success"]

                    ResultMatrix[3].append(outcome)
                    expectationtype=  keycomponents[j]["expectation_config"]["expectation_type"]
                    ResultMatrix[4].append(expectationtype)
                    expectationnotes= keycomponents[j]["expectation_config"]["meta"]["notes"]["content"]
                    ResultMatrix[5].append(expectationnotes)
                    #Results changes based on how complex expectation is, so we iterate through first two dictionaries in the context to fin out context columns and values expected
                    Counter = 2
                    Token = 0
                    observedvalues = []
                    expectedvalues = []
                    missingvalues = []

                    for k in keycomponents[j]["expectation_config"]["kwargs"]:
                        Token = Token + 1
                        ResultMatrix[5 + Token].append(keycomponents[j]["expectation_config"]["kwargs"][k])
                        
                        if Counter == Token:
                            expectedvalues=keycomponents[j]["expectation_config"]["kwargs"][k]
                            break

                   
                    
                    if (expectationtype == "expect_column_distinct_values_to_equal_set" or expectationtype=="expect_column_distinct_values_to_be_in_set" ):
                        if "observed_value" in keycomponents[j]["result"]:
                            observedvalues =keycomponents[j]["result"]["observed_value"]
                        else:
                            observedvalues ={}

                        expectedset = set(expectedvalues)
                        observedset = set(observedvalues)
                        if (len(observedvalues) >0):
                            missingset= set()  
                            try:
                                
                                if expectationnotes=="Country should match Countries list" or expectationnotes=="Warehouse value should match warehouses list":
                                    if (observedset.isdisjoint(expectedset)):
                                        missingset= observedset
                                    elif observedset.issubset(expectedset) == False:
                                        unionboth= observedset | expectedset
                                        missingset= unionboth - expectedset  
                                    # No need to add case above for expected set to be superset that is correct                                           
                                elif expectationnotes=="Each Periods must be in the file": 
                                    
                                    if (observedset.isdisjoint(expectedset)):
                                        missingset=observedset
                                    elif observedset.issubset(expectedset):
                                        missingset=observedset ^ expectedset                                       
                                    else:
                                        unionboth= observedset | expectedset
                                        missingset= (unionboth - expectedset )|(expectedset- observedset)                                      
                                elif expectationnotes=="Each Accountcode in set must be in the file":
                                    missingset = observedset ^ expectedset
                                else:
                                    missingset = observedset ^ expectedset
                                if missingset != set():
                                    missingvalues = list(sorted(missingset))
                            except:
                                print ("{Failures: Observed list :",observedset ,", Expected set :", expectedset)
                            
                    if len(observedvalues) > 0: 
                        ResultMatrix[8].append(observedvalues)
                    else :
                        ResultMatrix[8].append("")
                        
                   
                    if len(missingvalues) > 0: 
                        ResultMatrix[9].append(missingvalues)
                    else :
                        ResultMatrix[9].append("")
                    
                    
                    ResultMatrix[10].append(
                        keycomponents[j]["expectation_config"]["kwargs"]
                    )
                    ResultMatrix[11].append(keycomponents[j]["result"])
                    

dataframe3 = pd.DataFrame(
    {
        "File Name": FilesMatrix[4],
        "#unsuccessful_expectations": statistics[0],
        "#successful_expectations": statistics[1],
        "ValidationTime": statistics[2],
        "Expecation Suite Name": statistics[3],
    }
)
# dataframe.to_csv(
#     f"{baseurl}FilesFailed.csv", sep=",", index=False, mode=mode, header=header
# )


dataframe4 = pd.DataFrame(
    {
        "Expectation_suite": ResultMatrix[0],
        "Filename": ResultMatrix[1],
        "Validation_time": ResultMatrix[2],
        "Outcome": ResultMatrix[3],
        "Expectation_type": ResultMatrix[4],
        "ExpectationContext": ResultMatrix[5],
        "Columns": ResultMatrix[6],
        "Expected Values": ResultMatrix[7],
        "Observed Values": ResultMatrix[8],
        "Mismatched Values": ResultMatrix[9],
        "Expectation": ResultMatrix[10],
        "Validationresult": ResultMatrix[11],
    }
)
# dataframe.to_csv(
#         f"{baseurl}FailedResults.csv", sep=",", index=False, header=header, mode=mode
#     )

with pd.ExcelWriter(f"{baseurl}ValidationResults.xlsx") as writer:
    dataframe1.to_excel(writer, sheet_name="FilesNotFound", index=False, header=header
    )

    dataframe2.to_excel(
        writer, sheet_name="FilesSuccessful", index=False, header=header
    )

    dataframe3.to_excel(
        writer, sheet_name="Files Failed", index=False, header=header
    )

    dataframe4.to_excel(
        writer, sheet_name="Results", index=False, header=header,
    )
