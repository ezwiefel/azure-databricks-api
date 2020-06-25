# Azure Databricks API Wrapper
A Python, object-oriented wrapper for the [Azure Databricks REST API 2.0](https://docs.azuredatabricks.net/api/latest/index.html)

[![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/ezwiefel/azure-databricks-api/Unit%20Tests/main?style=for-the-badge)](https://github.com/ezwiefel/azure-databricks-api/actions?query=workflow%3A%22Unit+Tests%22)
[![Coveralls github](https://img.shields.io/coveralls/github/ezwiefel/azure-databricks-api?style=for-the-badge)](https://coveralls.io/github/ezwiefel/azure-databricks-api)
[![PyPI](https://img.shields.io/pypi/v/azure-databricks-api?style=for-the-badge)](https://pypi.org/project/azure-databricks-api/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/azure-databricks-api?label=PIP%20DOWNLOADS&style=for-the-badge)](https://pypi.org/project/azure-databricks-api/)
[![GitHub](https://img.shields.io/github/license/ezwiefel/azure-databricks-api?style=for-the-badge)](https://github.com/ezwiefel/azure-databricks-api/blob/main/LICENSE)

### Installation
This package is pip installable.
```bash
pip install azure-databricks-api
```

### Implemented APIs
As of June 25th, 2020 there are 12 different services available in the Azure Databricks API. Currently, the following services are supported by the Azure Databricks API Wrapper.
* [x] Clusters
* [ ] Cluster Policies _(Preview)_
* [x] DBFS
* [x] Groups _(Must be Databricks admin)_
* [ ] Instance Pools
* [ ] Jobs
* [X] Libraries
* [ ] MLflow
* [ ] SCIM _(Preview)_
* [ ] Secrets
* [x] Token
* [x] Workspace


### Client Instantiation
To create the client object, you pass the Azure region your workspace is located in and the [generated Personal Access Token](https://docs.databricks.com/api/latest/authentication.html#generate-a-token)
```python
from azure_databricks_api import AzureDatabricksRESTClient

azure_region = '[INSERT YOUR AZURE REGION HERE]'
token = '[INSERT YOUR PERSONAL ACCESS TOKEN]' 

client = AzureDatabricksRESTClient(region=azure_region, token=token)
```

### Clusters Client Usage
The services above are implemented as children objects of the client. For example, to pin a cluster, you can either pass the `cluster_name` or `cluster_id` to the `pin()` method:
```python
client.clusters.pin(cluster_name='test_cluster_name')
```

The other services are implemented similarly. (e.g. `client.tokens` or `client.groups`) 

