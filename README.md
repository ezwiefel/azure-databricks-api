<!---
 Copyright (c) 2018 Microsoft
 
 This software is released under the MIT License.
 https://opensource.org/licenses/MIT
-->

# Azure Databricks API Wrapper
A Python, object-oriented wrapper for the [Azure Databricks REST API 2.0](https://docs.azuredatabricks.net/api/latest/index.html)

### Installation
This package is pip installable.
```bash
pip install azure-databricks-cli
```

### Implemented APIs
As of August 10th, 2018 there are 8 different services available in the Azure Databricks API. Currently, the following 
services are supported by the Azure Databricks API Wrapper.
* [x] Clusters
* [x] Groups
* [x] Token
* [ ] DBFS
* [ ] Jobs
* [ ] Libraries
* [ ] Secrets
* [ ] Workspace

### Client Instantiation
To create the client object, you pass the Azure region your workspace is located in and the [generated Personal Access Token](https://docs.databricks.com/api/latest/authentication.html#generate-a-token)
```python
from azure_databricks_cli import AzureDatabricksRESTClient

azure_region = 'centralus'
token = '[INSERT YOUR PERSONAL ACCESS TOKEN]' 

client = AzureDatabricksRESTClient(region=azure_region, token=token)
```

### Clusters Client Usage
The services above are implements as children objects of the client. For example, to pin a cluster, you can either pass the cluster_name or cluster_id:
```python
client.clusters.pin('test_cluster_name')
```

The other services are implemented similarly. (e.g. `client.tokens` or `client.groups`) 

