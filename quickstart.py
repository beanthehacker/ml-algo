import os
import time
from datetime import datetime, timezone

from azure.ai.anomalydetector import AnomalyDetectorClient
from azure.ai.anomalydetector.models import DetectionRequest, ModelInfo, LastDetectionRequest
from azure.ai.anomalydetector.models import ModelStatus, DetectionStatus
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import HttpResponseError

SUBSCRIPTION_KEY = "84d38f5a85654c9eb31afee50efe07e2"
ANOMALY_DETECTOR_ENDPOINT = ""https://lg-test-8feb2023.cognitiveservices.azure.com/"
DATA_SOURCE = os.environ["ANOMALY_DETECTOR_DATA_SOURCE"]

ad_client = AnomalyDetectorClient(AzureKeyCredential(SUBSCRIPTION_KEY), ANOMALY_DETECTOR_ENDPOINT)
model_list = list(ad_client.list_multivariate_model(skip=0, top=10000))
print("{:d} available models before training.".format(len(model_list)))

print("Training new model...(it may take a few minutes)")
data_feed = ModelInfo(start_time=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc), end_time=datetime(2021, 1, 2, 12, 0, 0, tzinfo=timezone.utc), source=DATA_SOURCE)
response_header = \
        ad_client.train_multivariate_model(data_feed, cls=lambda *args: [args[i] for i in range(len(args))])[-1]
trained_model_id = response_header['Location'].split("/")[-1]

model_status = None

while model_status != ModelStatus.READY and model_status != ModelStatus.FAILED:
    model_info = ad_client.get_multivariate_model(trained_model_id).model_info
    model_status = model_info.status
    time.sleep(30)
    print ("MODEL STATUS: " + model_status)

if model_status == ModelStatus.READY:
            new_model_list = list(ad_client.list_multivariate_model(skip=0, top=10000))
            print("Model training complete.\n--------------------")
            print("{:d} available models after training.".format(len(new_model_list)))
            print("New Model ID " + trained_model_id)

detection_req = DetectionRequest(source=DATA_SOURCE, start_time=datetime(2021, 1, 2, 12, 0, 0, tzinfo=timezone.utc), end_time=datetime(2021, 1, 3, 0, 0, 0, tzinfo=timezone.utc))
response_header = ad_client.detect_anomaly(trained_model_id, detection_req, cls=lambda *args: [args[i] for i in range(len(args))])[-1]
result_id = response_header['Location'].split("/")[-1]

# Get results (may need a few seconds)
r = ad_client.get_detection_result(result_id)
print("Get detection result...(it may take a few seconds)")

while r.summary.status != DetectionStatus.READY and r.summary.status != DetectionStatus.FAILED:
    r = ad_client.get_detection_result(result_id)
    time.sleep(1)

print("Result ID:\t", r.result_id)
print("Result status:\t", r.summary.status)
print("Result length:\t", len(r.results))
print("\nAnomaly details:")
for i in r.results:
        if i.value.is_anomaly:
            print("timestamp: {}, is_anomaly: {:<5}, anomaly score: {:.4f}, severity: {:.4f}, contributor count: {:<4d}".format(i.timestamp, str(i.value.is_anomaly), i.value.score, i.value.severity, len(i.value.interpretation) if i.value.is_anomaly else 0))
            if i.value.interpretation is not None:
                for interp in i.value.interpretation:
                    print("\tcorrelation changes: {:<10}, contribution score: {:.4f}".format(interp.variable, interp.contribution_score))