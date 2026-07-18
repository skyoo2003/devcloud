"""Regression tests: unimplemented operations must return an AWS error, not a
silent 200 empty-body success. Guards the fix in the services that previously
returned a false 200 for unknown ops (glue, sagemaker, codebuild, iot, ...)."""

import pytest
from botocore.exceptions import ClientError


def test_glue_unimplemented_op_errors(glue_client):
    with pytest.raises(ClientError):
        glue_client.list_workflows()


def test_sagemaker_unimplemented_op_errors(sagemaker_client):
    with pytest.raises(ClientError):
        sagemaker_client.list_auto_ml_jobs()


def test_codebuild_unimplemented_op_errors(codebuild_client):
    with pytest.raises(ClientError):
        codebuild_client.list_build_batches()
