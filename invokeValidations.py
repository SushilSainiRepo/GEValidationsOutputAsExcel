import Modules as md
import datetime as dt
import great_expectations as ge
import pandas as pd

Validations = md.FileValidationsHandler()
# Validations.datafilesfolder=f'demoDynamicValidation/metadata/'
# Validations.metadatafilesfolder=f'demoDynamicValidation/files/'
Validations.createcheckpoint("DataQuality")
FilesMatrix = Validations.runcheckpoint(CheckPointName="DataQuality", opendocs=True)
