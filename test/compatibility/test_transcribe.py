def test_list_transcription_jobs(transcribe_client):
    resp = transcribe_client.list_transcription_jobs()
    assert "TranscriptionJobSummaries" in resp
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
