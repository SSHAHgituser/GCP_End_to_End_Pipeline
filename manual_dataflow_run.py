import json
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


PROJECT_ID = "end-to-end-de-pipeline"
SUBSCRIPTION = "projects/end-to-end-de-pipeline/subscriptions/air-quality-sub"
BQ_TABLE = "end-to-end-de-pipeline:air_quality.waqi_hyd_bronze"


class ParseWAQI(beam.DoFn):
    def process(self, message):
        msg = json.loads(message.decode("utf-8"))
        data = msg["raw_payload"]["data"]
        iaqi = data.get("iaqi", {})
        city = data.get("city", {})

        event_time = datetime.fromisoformat(msg["event_time"])

        yield {
            "event_date": event_time.date().isoformat(),
            "city": msg["city"],
            "station_name": city.get("name"),
            "lat": city.get("geo", [None, None])[0],
            "lon": city.get("geo", [None, None])[1],

            "aqi": data.get("aqi"),
            "dominant_pollutant": data.get("dominentpol"),

            "pm25": iaqi.get("pm25", {}).get("v"),
            "pm10": iaqi.get("pm10", {}).get("v"),
            "co": iaqi.get("co", {}).get("v"),
            "no2": iaqi.get("no2", {}).get("v"),
            "so2": iaqi.get("so2", {}).get("v"),

            "temperature": iaqi.get("t", {}).get("v"),
            "humidity": iaqi.get("h", {}).get("v"),
            "wind": iaqi.get("w", {}).get("v"),

            "event_time": event_time.isoformat(),
            "bq_load_time": datetime.utcnow().isoformat()
        }


def run():
    options = PipelineOptions(
        streaming=True,
        project=PROJECT_ID,
        region="asia-south2",
        # worker_region="asia-south1",   # AUTO SELECT ZONE - Code not working in mumbai region
        temp_location="gs://source_data_dataflow/temp",
        staging_location="gs://source_data_dataflow/staging",
        job_name="waqi-streaming-bronze"
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read PubSub" >> beam.io.ReadFromPubSub(
                subscription=SUBSCRIPTION
            )
            | "Parse JSON" >> beam.ParDo(ParseWAQI())
            | "Write to BQ" >> beam.io.WriteToBigQuery(
                table=BQ_TABLE,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )


if __name__ == "__main__":
    run()
