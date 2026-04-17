import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_cache_cluster(elasticache_client):
    resp = elasticache_client.create_cache_cluster(
        CacheClusterId="compat-cache",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    assert resp["CacheCluster"]["CacheClusterId"] == "compat-cache"

    desc = elasticache_client.describe_cache_clusters(CacheClusterId="compat-cache")
    assert len(desc["CacheClusters"]) == 1


def test_delete_cache_cluster(elasticache_client):
    elasticache_client.create_cache_cluster(
        CacheClusterId="del-cache",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    elasticache_client.delete_cache_cluster(CacheClusterId="del-cache")


def test_create_replication_group(elasticache_client):
    resp = elasticache_client.create_replication_group(
        ReplicationGroupId="compat-rg",
        ReplicationGroupDescription="test replication group",
    )
    assert resp["ReplicationGroup"]["ReplicationGroupId"] == "compat-rg"

    desc = elasticache_client.describe_replication_groups(
        ReplicationGroupId="compat-rg"
    )
    assert len(desc["ReplicationGroups"]) == 1


def test_describe_nonexistent_cluster(elasticache_client):
    with pytest.raises(ClientError) as exc:
        elasticache_client.describe_cache_clusters(CacheClusterId="no-such-cache")
    assert exc.value.response["Error"]["Code"] == "CacheClusterNotFound"
