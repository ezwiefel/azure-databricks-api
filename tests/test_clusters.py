import pytest

from azure_databricks_api.exceptions import ResourceDoesNotExist, InvalidState
from tests.utils import create_client
import uuid
from time import sleep

client = create_client()


def teardown_module(module):
    """Permanently delete all clusters at the end of the test"""
    clusters = client.clusters.list()

    for cluster in clusters:
        client.clusters.unpin(cluster_id=cluster['cluster_id'])
        client.clusters.permanent_delete(cluster_id=cluster['cluster_id'])


@pytest.fixture(scope='module')
def cluster_name():
    return str(uuid.uuid4())


@pytest.fixture(scope="function")
def cluster_id(cluster_name):
    cluster = client.clusters.get(cluster_name=cluster_name)
    return cluster['cluster_id']


@pytest.fixture(scope='module')
def spark_versions():
    spark_versions = client.clusters.spark_versions()

    assert len(spark_versions) > 0
    return spark_versions


@pytest.fixture(scope='module')
def node_types():
    node_types = client.clusters.list_node_types()

    assert len(node_types) > 0
    return node_types


@pytest.fixture(scope="module")
def smallest_node(node_types):
    sorted_nodes = sorted(node_types, key=lambda x: x['num_cores'])

    return sorted_nodes[0]['node_type_id']


def test_create_cluster_invalid_spark_version_raises(cluster_name, smallest_node):
    with pytest.raises(ValueError):
        _ = client.clusters.create(cluster_name=cluster_name, num_workers=0,
                                   spark_version="dask", node_type_id=smallest_node)


def test_create_cluster_invalid_node_raises_error(cluster_name, spark_versions):
    with pytest.raises(ValueError):
        _ = client.clusters.create(cluster_name=cluster_name, num_workers=0,
                                   spark_version=list(spark_versions.keys())[0],
                                   node_type_id='Super_Big_with_lots_of_power')


def test_create_cluster(cluster_name, spark_versions, smallest_node):
    cluster = client.clusters.create(cluster_name=cluster_name, num_workers=0,
                                     spark_version=list(spark_versions.keys())[0], node_type_id=smallest_node)


def test_pin_cluster_by_name(cluster_name):
    client.clusters.pin(cluster_name=cluster_name)


def test_delete_pinned_cluster_raises_error(cluster_name):
    with pytest.raises(InvalidState):
        client.clusters.permanent_delete(cluster_name=cluster_name)


def test_unpin_cluster_by_name(cluster_name):
    client.clusters.unpin(cluster_name=cluster_name)


def test_pin_cluster_by_id(cluster_id):
    client.clusters.pin(cluster_id=cluster_id)


def test_unpin_clusters_by_id(cluster_id):
    client.clusters.unpin(cluster_id=cluster_id)


def test_terminate_cluster_by_name(cluster_name):
    client.clusters.terminate(cluster_name=cluster_name)


def test_start_cluster_by_name(cluster_name):
    """For some reason, this test seems to fail intermittently based on server state"""
    sleep_time = 5
    num_retries = 5
    for attempt in range(num_retries):
        try:
            client.clusters.start(cluster_name=cluster_name)
            break
        except InvalidState as state_error:
            if attempt == num_retries:
                raise state_error
            else:
                sleep(sleep_time)
                sleep_time += 2

def test_start_cluster_no_cluster_supplied():
    with pytest.raises(ValueError):
        client.clusters.start()


@pytest.mark.skip(msg="This test isn't feasible to perform based on timing of cluster start")
def test_restart_cluster_by_name(cluster_name):
    client.clusters.restart(cluster_name=cluster_name)


@pytest.mark.skip(msg="This test isn't feasible to perform based on timing of cluster start")
def test_terminate_cluster_by_id(cluster_id):
    client.clusters.terminate(cluster_name=cluster_id)


@pytest.mark.skip(msg="This test isn't feasible to perform based on timing of cluster start")
def test_start_cluster_by_id(cluster_id):
    client.clusters.start(cluster_id=cluster_id)


@pytest.mark.skip(msg="This test isn't feasible to perform based on timing of cluster start")
def test_restart_cluster_by_id(cluster_id):
    client.clusters.restart(cluster_id=cluster_id)


def test_list_clusters():
    cluster_list = client.clusters.list()
    assert len(cluster_list) > 0


def test_get_cluster(cluster_name):
    cluster = client.clusters.get(cluster_name=cluster_name)


def test_permanent_delete_all_clusters():
    clusters = client.clusters.list()

    for cluster in clusters:
        client.clusters.permanent_delete(cluster_id=cluster['cluster_id'])


def test_cluster_get_no_clusters(cluster_name):
    with pytest.raises(ResourceDoesNotExist):
        client.clusters.get(cluster_name=cluster_name)


def test_cluster_list_no_clusters():
    cluster_list = client.clusters.list()

    assert len(cluster_list) == 0
