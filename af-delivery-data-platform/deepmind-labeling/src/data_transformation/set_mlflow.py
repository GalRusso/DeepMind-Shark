import re
from typing import Any

import mlflow
from mlflow.entities.span import LiveSpan
from mlflow.gemini import autolog as gemini_autolog
from mlflow.langchain import autolog as langchain_autolog
from mlflow.openai import autolog as openai_autolog

mlflow.set_tracking_uri(uri="databricks")
mlflow.set_registry_uri(uri="databricks-uc")
# mlflow.set_experiment(experiment_id="2369490745915484") # test one
# Create or use experiment by name instead of hardcoded ID
mlflow.set_experiment(experiment_name="/Users/asafso@activefence.com/deepmind-labeling")

langchain_autolog()
gemini_autolog()
openai_autolog()


def _crop(val: Any, max_len=10):
    if isinstance(val, str):
        match = re.match(r"(data:[^;]+;base64,)(.+)", val or "")
        if not match:
            return val
        prefix, b64data = match.groups()
        cropped_b64 = b64data[:5] + "...<cropped>" if len(b64data) > 8 else b64data
        redacted_input = prefix + cropped_b64
        return redacted_input
    if isinstance(val, dict):
        return {k: _crop(v, max_len) for k, v in val.items()}
    if isinstance(val, list):
        return [_crop(v, max_len) for v in val]
    return val


def crop_media_url(span: LiveSpan) -> None:
    # Inputs / outputs
    if getattr(span, "inputs", None) is not None:
        span.set_inputs(_crop(span.inputs))
    if getattr(span, "outputs", None) is not None:
        span.set_outputs(_crop(span.outputs))
    # Attributes (optional)
    if getattr(span, "attributes", None):
        span.set_attributes(_crop(span.attributes))


mlflow.tracing.configure(span_processors=[crop_media_url])
