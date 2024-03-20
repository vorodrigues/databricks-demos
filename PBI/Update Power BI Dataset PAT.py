# Databricks notebook source
# MAGIC %pip install azure-identity

# COMMAND ----------

import json, requests, azure.identity

class PbiEmbedService(object):

    def create_credential(self, tenant_id, client_id, client_secret, user_login = False):
        if user_login:
          self.credential = azure.identity.DeviceCodeCredential()
        else:
          self.credential = azure.identity.ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

    def get_access_token(self):
        access_token_class = self.credential.get_token('https://analysis.windows.net/powerbi/api/.default')
        return access_token_class.token

    def get_request_header(self):
        """
        Get Power BI API request header

        Returns:
            Dict: Request header
        """

        return {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.get_access_token()}

    def update_dataset_pat(self, workspace_id, dataset_id, pat):
        headers = self.get_request_header()
        r = requests.post(f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/Default.TakeOver", headers = headers)

        _dsobject = requests.get(f" https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/datasources", headers = headers)
        _dso = json.loads(_dsobject.text)
        datasource_id =_dso["value"][0]["datasourceId"]
        gateway_id = _dso["value"][0]["gatewayId"]

        requestBodyJson = {
          "credentialDetails": {
          "credentialType": "Key",
          "credentials": f'{{"credentialData":[{{"name":"key", "value": "{pat}"}}]}}',
          "encryptedConnection": "Encrypted",
          "encryptionAlgorithm": "None",
          "privacyLevel": "None"
         }
        }
        update_pat = requests.patch(f"https://api.powerbi.com/v1.0/myorg/gateways/{gateway_id}/datasources/{datasource_id}", json=requestBodyJson, headers = headers)
        return update_pat
      
    def create_dataset_oauth(self, workspace_id):
        headers = self.get_request_header()
        r = requests.post(f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/Default.TakeOver", headers = headers)

        _dsobject = requests.get(f" https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/datasources", headers = headers)
        _dso = json.loads(_dsobject.text)
        datasource_id =_dso["value"][0]["datasourceId"]
        gateway_id = _dso["value"][0]["gatewayId"]

        requestBodyJson = {
          "credentialDetails": {
          "credentialType": "Key",
          "credentials": f'{{"credentialData":[{{"name":"key", "value": "{pat}"}}]}}',
          "encryptedConnection": "Encrypted",
          "encryptionAlgorithm": "None",
          "privacyLevel": "None"
         }
        }
        update_pat = requests.patch(f"https://api.powerbi.com/v1.0/myorg/gateways/{gateway_id}/datasources/{datasource_id}", json=requestBodyJson, headers = headers)
        return update_pat


# COMMAND ----------

client_id = "42615338-85a6-434f-b2c6-060b2e1c7d88"
client_secret= dbutils.secrets.get("myscope", "mykey")
tenant_id='be2315e6-ec05-4a2a-95c2-f94f395a97c5'
dataset_id='d3c51194-f55e-41d9-acdc-f16f2147902c'
workspace_id ='fa4e1dc3-0657-41c8-a613-849f1cf9aaaf'
pat = "<pat goes here>"


pbi_svc = PbiEmbedService()
pbi_svc.create_credential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret, user_login = False)
r = pbi_svc.update_dataset_pat(workspace_id , dataset_id, pat)
r.text

# COMMAND ----------

pbi_svc = PbiEmbedService()
pbi_svc.create_credential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret, user_login = False)
r = pbi_svc.update_dataset_pat(workspace_id , dataset_id, pat)
r.text
