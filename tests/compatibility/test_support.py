def test_describe_services(support_client):
    resp = support_client.describe_services()
    assert "services" in resp
    assert len(resp["services"]) >= 1
    codes = [s["code"] for s in resp["services"]]
    assert "general-info" in codes


def test_describe_severity_levels(support_client):
    resp = support_client.describe_severity_levels()
    assert "severityLevels" in resp
    codes = [item["code"] for item in resp["severityLevels"]]
    assert "low" in codes
    assert "critical" in codes


def test_describe_trusted_advisor_checks(support_client):
    resp = support_client.describe_trusted_advisor_checks(language="en")
    assert "checks" in resp
    assert isinstance(resp["checks"], list)


def test_create_and_describe_case(support_client):
    resp = support_client.create_case(
        subject="Test case",
        serviceCode="general-info",
        categoryCode="other",
        severityCode="low",
        communicationBody="This is a test case",
    )
    case_id = resp["caseId"]
    assert case_id

    desc = support_client.describe_cases(caseIdList=[case_id])
    assert len(desc["cases"]) == 1
    assert desc["cases"][0]["subject"] == "Test case"


def test_describe_communications(support_client):
    resp = support_client.create_case(
        subject="Comms",
        serviceCode="general-info",
        categoryCode="other",
        severityCode="low",
        communicationBody="Hi",
    )
    case_id = resp["caseId"]
    support_client.add_communication_to_case(
        caseId=case_id,
        communicationBody="follow-up",
    )
    out = support_client.describe_communications(caseId=case_id)
    assert "communications" in out


def test_resolve_case(support_client):
    resp = support_client.create_case(
        subject="Resolve me",
        serviceCode="general-info",
        categoryCode="other",
        severityCode="low",
        communicationBody="Initial",
    )
    case_id = resp["caseId"]
    r = support_client.resolve_case(caseId=case_id)
    assert r["finalCaseStatus"] == "resolved"


def test_describe_trusted_advisor_check_result(support_client):
    resp = support_client.describe_trusted_advisor_check_result(checkId="abc")
    assert "result" in resp
    assert resp["result"]["checkId"] == "abc"


def test_describe_attachment(support_client):
    resp = support_client.describe_attachment(attachmentId="does-not-exist")
    assert "attachment" in resp


def test_describe_supported_languages(support_client):
    resp = support_client.describe_supported_languages(
        issueType="customer-service",
        serviceCode="general-info",
        categoryCode="other",
    )
    assert "supportedLanguages" in resp
    assert len(resp["supportedLanguages"]) >= 1
