import great_expectations as ge
import pandas as pd
import os as os

ConfigfilePath = f"DynamicValidation/metadata/ValidationsConfig.xlsx"
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)
from great_expectations.exceptions import DataContextError

# from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest


context = ge.data_context.DataContext()
# datasource = context.get_datasource("demo2_datasource")
files = pd.read_excel(
    f"DynamicValidation/metadata/ValidationsConfig.xlsx", sheet_name="FilesList"
)
Accounts = pd.read_excel(
    f"DynamicValidation/metadata/ValidationsConfig.xlsx", sheet_name="Accounts"
)
Columns = pd.read_excel(
    f"DynamicValidation/metadata/ValidationsConfig.xlsx", sheet_name="Columns"
)
Countries = pd.read_excel(
    f"DynamicValidation/metadata/ValidationsConfig.xlsx", sheet_name="CountryCode"
)
Warehouses = pd.read_excel(
    f"DynamicValidation/metadata/ValidationsConfig.xlsx", sheet_name="Warehouse"
)

warehouselist = Warehouses["warehousecode"].to_list()
countrieslist = Countries["countrycode"].to_list()
# File1=pd.read_csv(f'demoDynamicValidation\metadata\File1.csv')
# print(Countries)
# print(Warehouses)
# print(Periods)
for id, filedetail in files.iterrows():
    fileIdentifier = filedetail["fileidentifier"]
    filename = filedetail["filename"]
    warehouseColumn = filedetail["Warehouse"]
    countryColumn = filedetail["Country"]
    accountsColumn = filedetail["Accounts"]


    print("FileIdentity =", fileIdentifier, "filename =", filename)
    # Context for Expectation Suite for each file is instantiated
    expectation_suite_name = fileIdentifier

    try:
        suite = context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        print(
            f'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.'
        )
        context.delete_expectation_suite(expectation_suite_name=expectation_suite_name)
        suite = context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
    except DataContextError:
        suite = context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')


    if filename == 'P14010_Retro_Funding.csv':
        print("Starting custom Retrofunding expectation generation")

        expectation_columns_DateComparison = ExpectationConfiguration(
            **{
                "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
                "kwargs": {
                    "allow_cross_type_comparisons": False,
                    "column_A": "EndDate",
                    "column_B": "StartDate",
                    "ignore_row_if": "neither",
                    "or_equal": True
                },
                "meta": {
                    "notes": {
                        "format": "markdown",
                        "content": "Retrofunding Start Dates larger than End dates have been found",
                    }
                },
            },
        )
        suite.add_expectation(
                expectation_configuration=expectation_columns_DateComparison,
                overwrite_existing=True,
            )
    # find columns for file and set expectation to check if columns exists in file
    ColumnsForFile = Columns[Columns["fileidentifier"] == fileIdentifier]
    print(chr(10), chr(13))
    if ColumnsForFile.count()[0] > 0:
        print("Columns list", ColumnsForFile["column"].tolist(), chr(10), chr(13))

        columnslist = ColumnsForFile["column"].tolist()
        expectation_columns_configuration = ExpectationConfiguration(
            **{
                "expectation_type": "expect_table_columns_to_match_set",
                "kwargs": {"column_set": columnslist},
                "meta": {
                    "notes": {
                        "format": "markdown",
                        "content": "Column headers should match names",
                    }
                },
            }
        )

        suite.add_expectation(
            expectation_configuration=expectation_columns_configuration,
            overwrite_existing=True,
        )
        # find columns which should be unique and set expectation to check no duplicates
        UniqueColumnsForFile = ColumnsForFile[ColumnsForFile["unique"] == 1]
        if UniqueColumnsForFile.count()[0] > 0:
            UniqueColumnslist = UniqueColumnsForFile["column"].tolist()
            markdown = f"Check no duplicates in [{UniqueColumnslist}]"
            print("Unique Columns", UniqueColumnslist, chr(10), chr(13))
            expectation_Uniquecolumns_configuration = ExpectationConfiguration(
                **{
                    "expectation_type": "expect_compound_columns_to_be_unique",
                    "kwargs": {"column_list": UniqueColumnslist},
                    "meta": {"notes": {"format": "markdown", "content": markdown}},
                }
            )
            suite.add_expectation(
                expectation_configuration=expectation_Uniquecolumns_configuration,
                overwrite_existing=True,
            )

        # NotNullsColumnsForFile = ColumnsForFile[ColumnsForFile["NoNulls"]=="YES" ]

        # NoNullsColumnsList = NotNullsColumnsForFile["Column"].tolist()

        for Colid, Columndetail in ColumnsForFile.iterrows():
            columnName = Columndetail["column"]
            if Columndetail["AllowNulls"] == "NO":
                markdown = f"Check for nulls in {columnName}"
                expectation_NoNulls_configuration = ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_values_to_not_be_null",
                        "kwargs": {
                            "column": columnName,
                            "mostly": 1.0,
                        },
                        "meta": {"notes": {"format": "markdown", "content": markdown}},
                    }
                )
                suite.add_expectation(
                    expectation_configuration=expectation_NoNulls_configuration
                )
            if columnName == accountsColumn:
                AccountsForFile = Accounts[Accounts["fileidentifier"] == fileIdentifier]

                if AccountsForFile.count()[0] > 0:
                    AccountsList = AccountsForFile["accountcode"].tolist()
                    print("Accounts =", AccountsForFile["accountcode"].tolist())
                    expectation_Accounts_configuration = ExpectationConfiguration(
                        **{
                            "expectation_type": "expect_column_distinct_values_to_equal_set",
                            "kwargs": {
                                "column": "AccountCode",
                                "value_set": AccountsList,
                            },
                            "meta": {
                                "notes": {
                                    "format": "markdown",
                                    "content": "Each Accountcode in set must be in the file",
                                }
                            },
                        }
                    )
                    suite.add_expectation(
                        expectation_configuration=expectation_Accounts_configuration
                    )
                else:
                    print("Accounts = NA")

            elif columnName == "Period" or columnName == "FinancialYearMonthID":
                Periods = pd.read_excel(
                    f"DynamicValidation/metadata/ValidationsConfig.xlsx",
                    sheet_name="Periods",
                )
                PeriodsForFile = Periods[Periods["fileidentifier"] == fileIdentifier]
                startPeriod = 0
                EndPeriod = 0
                if PeriodsForFile.count()[0] > 0:
                    startPeriod = PeriodsForFile["startperiod"].values[0]
                    EndPeriod = PeriodsForFile["endperiod"].values[0]

                    missingPeriods=set()
                    if type(PeriodsForFile["missedperiods"].values[0]) == str :
                        tempArray= PeriodsForFile["missedperiods"].values[0].replace(' ','').split(",")
                        ListOfMissingPeriods = [int(numeric_string) for numeric_string in tempArray ]
                        missingPeriods = set(sorted(ListOfMissingPeriods))
        

                    CheckPeriods = []
                    for x in range(2017,2025):
                        for y in range(1, 13):
                            if x * 100 + y >= startPeriod and x * 100 + y <= EndPeriod:
                                period= set([x*100+y])
                                if missingPeriods.issuperset( period)==False : 
                                    CheckPeriods.append(x * 100 + y)
                                else : print (period , " is to be excluded ")
                    print(CheckPeriods)
                    expectation_Periods_configuration = ExpectationConfiguration(
                        **{
                            "expectation_type": "expect_column_distinct_values_to_equal_set",
                            "kwargs": {"column": columnName, "value_set": CheckPeriods},
                            "meta": {
                                "notes": {
                                    "format": "markdown",
                                    "content": "Each Periods must be in the file",
                                }
                            },
                        }
                    )
                    suite.add_expectation(
                        expectation_configuration=expectation_Periods_configuration
                    )
                else:
                    print("Periods = NA")
            elif columnName == warehouseColumn:
                expectation_Periods_configuration = ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_distinct_values_to_be_in_set",
                        "kwargs": {"column": columnName, "value_set": warehouselist},
                        "meta": {
                            "notes": {
                                "format": "markdown",
                                "content": "Warehouse value should match warehouses list",
                            }
                        },
                    }
                )
                suite.add_expectation(
                    expectation_configuration=expectation_Periods_configuration
                )
            elif columnName == countryColumn:
                expectation_Periods_configuration = ExpectationConfiguration(
                    **{
                        "expectation_type": "expect_column_distinct_values_to_be_in_set",
                        "kwargs": {"column": columnName, "value_set": countrieslist},
                        "meta": {
                            "notes": {
                                "format": "markdown",
                                "content": "Country should match Countries list",
                            }
                        },
                    }
                )
                suite.add_expectation(
                    expectation_configuration=expectation_Periods_configuration
                )

    context.save_expectation_suite(
        expectation_suite=suite, expectation_suite_name=expectation_suite_name
    )
    suite_identifier = ExpectationSuiteIdentifier(
        expectation_suite_name=expectation_suite_name
    )

    context.build_data_docs(resource_identifiers=[suite_identifier])
    print(chr(10), chr(13))


print("All the expectation suites are generated")
print(os.name)
if os.name == "nt":
    context.open_data_docs(resource_identifier=suite_identifier)
exit(0)


# print(context.get_expectation_suite(expectation_suite_name=expectation_suite_name))
