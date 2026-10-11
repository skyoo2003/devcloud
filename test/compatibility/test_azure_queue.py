import pytest
import requests
import xml.etree.ElementTree as ET
from conftest import DEVCLOUD_URL


@pytest.fixture
def queue_endpoint(devcloud_server):
    return f"{DEVCLOUD_URL}/devstoreaccount1/queue"


def test_azure_queue_lifecycle(queue_endpoint):
    queue_name = "test-compat-queue"
    headers = {
        "x-ms-version": "2023-11-03",
        "Authorization": "SharedKey devstoreaccount1:Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
    }

    # 1. Create Queue
    resp = requests.put(f"{queue_endpoint}/{queue_name}", headers=headers)
    assert resp.status_code in (201, 204)

    # 2. Duplicate create returns 204
    resp_dup = requests.put(f"{queue_endpoint}/{queue_name}", headers=headers)
    assert resp_dup.status_code == 204

    # 3. Get Queue Metadata
    resp_meta = requests.get(
        f"{queue_endpoint}/{queue_name}?comp=metadata", headers=headers
    )
    assert resp_meta.status_code == 200
    assert "x-ms-approximate-messages-count" in resp_meta.headers

    # 4. List Queues
    resp_list = requests.get(f"{queue_endpoint}?comp=list", headers=headers)
    assert resp_list.status_code == 200
    assert queue_name in resp_list.text

    # 5. Put Message
    msg_xml = (
        "<QueueMessage><MessageText>DevCloud Queue Message</MessageText></QueueMessage>"
    )
    resp_put = requests.post(
        f"{queue_endpoint}/{queue_name}/messages", headers=headers, data=msg_xml
    )
    assert resp_put.status_code == 201
    assert "<MessageId>" in resp_put.text

    # Parse message ID and pop receipt
    root = ET.fromstring(resp_put.text)
    msg_elem = root.find("QueueMessage")
    assert msg_elem is not None
    msg_id = msg_elem.find("MessageId").text
    pop_receipt = msg_elem.find("PopReceipt").text
    assert pop_receipt is not None

    # 6. Peek Messages
    resp_peek = requests.get(
        f"{queue_endpoint}/{queue_name}/messages?peekonly=true", headers=headers
    )
    assert resp_peek.status_code == 200
    assert "DevCloud Queue Message" in resp_peek.text

    # 7. Get Messages (Dequeues message)
    resp_get = requests.get(f"{queue_endpoint}/{queue_name}/messages", headers=headers)
    assert resp_get.status_code == 200
    assert "DevCloud Queue Message" in resp_get.text

    root_get = ET.fromstring(resp_get.text)
    new_pop_receipt = root_get.find("QueueMessage/PopReceipt").text

    # 8. Delete Message
    resp_del_msg = requests.delete(
        f"{queue_endpoint}/{queue_name}/messages/{msg_id}?popreceipt={new_pop_receipt}",
        headers=headers,
    )
    assert resp_del_msg.status_code == 204

    # 9. Clear Messages
    resp_clear = requests.delete(
        f"{queue_endpoint}/{queue_name}/messages", headers=headers
    )
    assert resp_clear.status_code == 204

    # 10. Delete Queue
    resp_del = requests.delete(f"{queue_endpoint}/{queue_name}", headers=headers)
    assert resp_del.status_code == 204

    # 11. Access deleted queue returns 404
    resp_deleted = requests.get(
        f"{queue_endpoint}/{queue_name}?comp=metadata", headers=headers
    )
    assert resp_deleted.status_code == 404
