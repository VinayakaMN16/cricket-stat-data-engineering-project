from googleapiclient.discovery import build
from datetime import datetime
import functions_framework

@functions_framework.cloud_event
def trigger_df_job(cloud_event):

    data = cloud_event.data

    bucket = data["bucket"]
    file_name = data["name"]

    input_file = f"gs://{bucket}/{file_name}"

    service = build("dataflow", "v1b3")

    project = "cricket-statistics-project"

    job_name = f"cricket-stats-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    template_body = {
        "jobName": job_name,
        "parameters": {
            "javascriptTextTransformGcsPath": "gs://js-json-dataflow/udf.js",
            "JSONPath": "gs://js-json-dataflow/bq.json",
            "javascriptTextTransformFunctionName": "transform",
            "outputTable": "cricket-statistics-project:ICC_Batsmen_data.crckt_dataset",
            "inputFilePattern": input_file,
            "bigQueryLoadingTemporaryDirectory": "gs://js-json-dataflow"
        }
    }

    response = (
        service.projects()
        .templates()
        .launch(
            projectId=project,
            gcsPath="gs://dataflow-templates-asia-south2/latest/GCS_Text_to_BigQuery",
            body=template_body
        )
        .execute()
    )

    print(response)