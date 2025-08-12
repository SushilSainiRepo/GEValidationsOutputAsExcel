import pandas as pd
import azure.storage.blob as blob

STORAGEACCOUNTURL = "https://X.blob.core.windows.net/"
STORAGEACCOUNTKEY = "X"
blob_service_client_instance = blob.BlobServiceClient(
    account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY
)
CONTAINERNAME = "finance-cost-files"
files = pd.read_excel(
    f"DynamicValidation/metadata/ValidationsConfig.xlsx",
    sheet_name="FilesList",
)
for id, filedetail in files.iterrows():
    fileIdentifier = filedetail["fileidentifier"]
    filename = filedetail["filename"]
    LOCALFILENAME = f"DynamicValidation/files/{filename}"
    BLOBNAME = f"MVP 2.0 Signed Off Input Files/{filename}"
    # download from blob
    # t1=time.time()
    try:
        blob_client_instance = blob_service_client_instance.get_blob_client(
            CONTAINERNAME, BLOBNAME, snapshot=None
        )
        with open(LOCALFILENAME, "wb") as my_blob:
            blob_data = blob_client_instance.download_blob()
            blob_data.readinto(my_blob)
    except:
        print(filename, " Cant be loaded, check if file doesn't exist or empty")
        continue
