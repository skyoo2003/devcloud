import importlib
import json
import os
import traceback


def handler(event, context):
    try:
        module, method = os.environ["DEVCLOUD_LAMBDA_HANDLER"].rsplit(".", 1)
        target = getattr(importlib.import_module(module.replace("/", ".")), method)
        result = target(event, context)
        return {"success": True, "payload": json.dumps(result, allow_nan=False)}
    except BaseException as exc:
        traceback.print_exc()
        return {
            "success": False,
            "error": {"errorType": type(exc).__name__, "errorMessage": str(exc)},
        }
