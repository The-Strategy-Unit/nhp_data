# function to update the sas tokens in databricks secrets
# in order to run this you will need the subscription ID, which can be found
# with
#  az account list --query "[].[id,name]" --output tsv
# make sure that you have logged in with all relevant subscriptions

function Update-SasTokens {
    param (
        [string]$subscription,
        [Alias('storage-account')]
        [string]$storageAccount
    )
  # create a SAS token that will expire in 24 hours.
  $expiry = (Get-Date).ToUniversalTime().AddHours(24).ToString('yyyy-MM-ddTHH:mm:ssZ')

  # generate the sas token for the inputs-data container
  $inputsSas = az storage container generate-sas `
    --auth-mode login --as-user `
    --subscription $subscription `
    --account-name $storageAccount `
    --name inputs-data `
    --permissions dc `
    --expiry $expiry `
    --https-only `
    --output tsv

  databricks secrets put-secret nhp MLCSU_INPUTS_DATA_ACCOUNT  --string-value $storageAccount
  databricks secrets put-secret nhp MLCSU_INPUTS_DATA_SAS --string-value $inputsSas

  # generate the sas token for the data container
  $dataSas = az storage container generate-sas `
    --auth-mode login --as-user `
    --subscription $subscription `
    --account-name $storageAccount `
    --name data `
    --permissions dc `
    --expiry $expiry `
    --https-only `
    --output tsv

  databricks secrets put-secret nhp MLCSU_DATA_ACCOUNT --string-value $storageAccount
  databricks secrets put-secret nhp MLCSU_DATA_SAS --string-value $dataSas
}