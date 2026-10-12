# SPDX-License-Identifier: Apache-2.0

import base64
import pytest
import requests
from conftest import DEVCLOUD_URL


@pytest.fixture
def gcp_base_url(devcloud_server):
    return DEVCLOUD_URL


def test_gcp_pubsub_lifecycle(gcp_base_url):
    project = "test-project-pubsub"
    topic_id = "user-signups"
    sub_id = "welcome-email-worker"
    headers = {
        "Authorization": "Bearer ya29.devcloud-pilot-token",
        "Content-Type": "application/json",
    }

    # 1. Create Topic
    resp = requests.put(
        f"{gcp_base_url}/v1/projects/{project}/topics/{topic_id}",
        headers=headers,
        json={"labels": {"env": "prod"}},
    )
    assert resp.status_code == 200
    topic_data = resp.json()
    assert topic_data["name"] == f"projects/{project}/topics/{topic_id}"

    # 2. Duplicate Create Topic -> 409 Conflict
    resp_dup = requests.put(
        f"{gcp_base_url}/v1/projects/{project}/topics/{topic_id}",
        headers=headers,
        json={},
    )
    assert resp_dup.status_code == 409

    # 3. Get Topic
    resp_get = requests.get(
        f"{gcp_base_url}/v1/projects/{project}/topics/{topic_id}",
        headers=headers,
    )
    assert resp_get.status_code == 200
    assert resp_get.json()["name"] == f"projects/{project}/topics/{topic_id}"

    # 4. List Topics
    resp_list = requests.get(
        f"{gcp_base_url}/v1/projects/{project}/topics",
        headers=headers,
    )
    assert resp_list.status_code == 200
    topics = [t["name"] for t in resp_list.json().get("topics", [])]
    assert f"projects/{project}/topics/{topic_id}" in topics

    # 5. Create Subscription
    resp_sub = requests.put(
        f"{gcp_base_url}/v1/projects/{project}/subscriptions/{sub_id}",
        headers=headers,
        json={
            "topic": f"projects/{project}/topics/{topic_id}",
            "ackDeadlineSeconds": 60,
        },
    )
    assert resp_sub.status_code == 200
    sub_data = resp_sub.json()
    assert sub_data["name"] == f"projects/{project}/subscriptions/{sub_id}"
    assert sub_data["topic"] == f"projects/{project}/topics/{topic_id}"

    # 6. Publish Messages
    msg_raw = "user=alex@example.com,signup=true"
    msg_b64 = base64.b64encode(msg_raw.encode("utf-8")).decode("utf-8")
    resp_pub = requests.post(
        f"{gcp_base_url}/v1/projects/{project}/topics/{topic_id}:publish",
        headers=headers,
        json={
            "messages": [
                {
                    "data": msg_b64,
                    "attributes": {"region": "ap-northeast-2"},
                }
            ]
        },
    )
    assert resp_pub.status_code == 200
    pub_data = resp_pub.json()
    assert len(pub_data.get("messageIds", [])) == 1

    # 7. Pull Messages
    resp_pull = requests.post(
        f"{gcp_base_url}/v1/projects/{project}/subscriptions/{sub_id}:pull",
        headers=headers,
        json={"maxMessages": 10},
    )
    assert resp_pull.status_code == 200
    pull_data = resp_pull.json()
    received = pull_data.get("receivedMessages", [])
    assert len(received) == 1

    rec_item = received[0]
    ack_id = rec_item["ackId"]
    assert ack_id != ""
    assert rec_item["message"]["data"] == msg_b64
    assert rec_item["message"]["attributes"]["region"] == "ap-northeast-2"

    # 8. Acknowledge Message
    resp_ack = requests.post(
        f"{gcp_base_url}/v1/projects/{project}/subscriptions/{sub_id}:acknowledge",
        headers=headers,
        json={"ackIds": [ack_id]},
    )
    assert resp_ack.status_code == 200

    # 9. Pull Again -> Empty
    resp_pull_empty = requests.post(
        f"{gcp_base_url}/v1/projects/{project}/subscriptions/{sub_id}:pull",
        headers=headers,
        json={"maxMessages": 10},
    )
    assert resp_pull_empty.status_code == 200
    assert len(resp_pull_empty.json().get("receivedMessages", [])) == 0

    # 10. Delete Subscription
    resp_del_sub = requests.delete(
        f"{gcp_base_url}/v1/projects/{project}/subscriptions/{sub_id}",
        headers=headers,
    )
    assert resp_del_sub.status_code == 200

    # 11. Delete Topic
    resp_del_topic = requests.delete(
        f"{gcp_base_url}/v1/projects/{project}/topics/{topic_id}",
        headers=headers,
    )
    assert resp_del_topic.status_code == 200
