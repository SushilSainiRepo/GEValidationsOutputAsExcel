from ruamel import yaml
import pandas as pd
import os as os
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from datetime import datetime


class FileValidationsHandler:
    metadatafilesfolder = f"DynamicValidation/metadata/"
    datafilesfolder = f"DynamicValidation/files/"

    context = ge.get_context()

    def opendoc(self):
        self.context.open_data_docs()
        return True

    def printmyname(self, stringname):
        print(stringname)
        return True

    def createcheckpoint(self, CheckPointName):
        checkpoint_config = {
            "name": CheckPointName,
            "config_version": 1,
            "class_name": "SimpleCheckpoint",
        }
        self.context.add_checkpoint(**checkpoint_config)
        return True

    def runcheckpoint(self, CheckPointName, opendocs):
        Allfiles = pd.read_excel(
            f"{self.metadatafilesfolder}ValidationsConfig.xlsx", sheet_name="FilesList"
        )
        # pd.read_csv(f'{self.metadatafilesfolder}FilesList.csv')

        FilesMatrix = []
        FilesMatrix.append([])  # Not found
        FilesMatrix.append([])  # PASS
        FilesMatrix.append([])  # FAIL
        FilesMatrix.append([])  # Collection of Results
        FilesMatrix.append([])  # COllection of RuniD
        FilesMatrix.append([])  # COllection of Filenames
        FilesMatrix.append([])  # file identifier
        FilesMatrix.append([])  # file encoding

        files= Allfiles[Allfiles["IsEnabled"]==True]
      
        for label, content in files.items():
            if label =="fileidentifier":
                for item in content:
                    FilesMatrix[5].append(item)
            if label =="filename":
                for item in content:
                    FilesMatrix[6].append(item)
            if label =="Encoding":
                for item in content:
                    FilesMatrix[7].append(item)



        for j in range(0,len(FilesMatrix[6])):
        
            filename = FilesMatrix[6][j]
            fileIdentifier = FilesMatrix[5][j]
            encoding = FilesMatrix[7][j]
            try:
                df = pd.read_csv(
                    f"{self.datafilesfolder}{filename}",
                    encoding=f"{encoding}",
                    #encoding='unicode_escape'
                    engine="python",
                )
            except Exception as e:
                print("File read exception : ",e)
                FilesMatrix[0].append(f"{filename} file not found")
            
                
               
                # print(filename, ' Cant be loaded, check if file doesn\'t exist or empty')
                continue
            try:
                df.columns.str.strip()
                # df  = df1.fillna(0)
               

                runid = filename
                #default_runtime_data_connector_name
                batch_request = RuntimeBatchRequest(
                    datasource_name="demo2_datasource",
                    data_connector_name="default_runtime_data_connector_name",
                    data_asset_name=fileIdentifier,  # This can be anything that identifies this data_asset for you
                    runtime_parameters={"batch_data": df},  # Pass your DataFrame here.
                    batch_identifiers={"default_identifier_name": filename},
                )
                
                results = self.context.run_checkpoint(
                    checkpoint_name=CheckPointName,
                    validations=[
                        {"batch_request": batch_request},
                    ],
                    expectation_suite_name=fileIdentifier,
                    run_name=runid,
                )
                print("stage2 :",filename)
                FilesMatrix[3].append(results)
                FilesMatrix[4].append(runid)

                    # print(f"Checkpoint Failed for {filename}")
            except Exception as e:
                print("File read exception : ",e)
                FilesMatrix[0].append(f"{filename} errored in validation due to GE internal issues")
                continue
            
            if results["success"]:
                FilesMatrix[1].append(filename)
                # print(f"Successfully Completed for {fileIdentifier}")
            else:
                FilesMatrix[2].append(filename)
                

        if os.name == "nt" and opendocs:
            self.opendoc()

        return FilesMatrix
