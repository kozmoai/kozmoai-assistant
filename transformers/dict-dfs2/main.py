import json
import logging
import os

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Union

import boto3
import pandas as pd
import requests

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.logging.formatter import BasePowertoolsFormatter
from aws_lambda_powertools.metrics import MetricUnit
from botocore.exceptions import ClientError


class CustomFormatter(BasePowertoolsFormatter):
    def __init__(self, log_record_order: Optional[List[str]] = None, *args, **kwargs):
        self.log_record_order = log_record_order or [
            "level",
            "location",
            "message",
            "timestamp",
        ]
        self.log_format = dict.fromkeys(self.log_record_order)
        super().__init__(*args, **kwargs)

    def append_keys(self, **additional_keys):
        # also used by `inject_lambda_context` decorator
        self.log_format.update(additional_keys)

    def current_keys(self) -> Dict[str, Any]:
        return self.log_format

    def remove_keys(self, keys: Iterable[str]):
        for key in keys:
            self.log_format.pop(key, None)

    def clear_state(self):
        self.log_format = dict.fromkeys(self.log_record_order)

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        """Format logging record as structured JSON str"""
        return json.dumps(
            {
                "event": super().format(record),
                "timestamp": self.formatTime(record),
                "lambda_name": "lbd_remed_annotation",
                **self.log_format,
            },
        )


_metrics = Metrics(namespace="lbd-remed-annotation")
_metrics.set_default_dimensions(environment="dev", another="one")
_env = os.getenv("ENVIRONMENT", "dev")
_logger = Logger(service="annotations_handler", logger_formatter=CustomFormatter())
_tracer = Tracer()


class OctavService:
    def __init__(self):
        self.octav_url = os.getenv(
            "URL_WESID",
            f"https://{_env}-api-interne.sacem.fr:443/wesid/v1/token/batch/EXMEDBatch?validity=60",
        )

    @_tracer.capture_method
    def get_temp_token(self) -> str:
        response = requests.get(self.octav_url)
        response.raise_for_status()
        data = response.json()
        return data["my_temp_wesid_token"]

    @_tracer.capture_method
    def _get_secret(self, secret_key: str) -> str:
        """Gets Octav Annotations Credentials."""
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name="eu-west-1")
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_key)
        except ClientError as e:
            raise e

        secret = get_secret_value_response["SecretString"]
        secret = json.loads(secret)
        client_secret = secret.get("client_secret")

        return client_secret

    @_tracer.capture_method
    def _send_post_request(self, url: str, body: dict) -> requests.Response:
        """Sends a POST request to the specified URL with the given body."""
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(url, data=body, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes

        return response

    @_tracer.capture_method
    def _get_token(self, url: str, secret_key: str):
        """Gets Octav Annotations token."""
        _logger.debug("Get token from Octav for REMED Annotations... ")
        body = {"pwd": self._get_secret(secret_key)}
        try:
            response = self._send_post_request(url, body)
            return response.text
        except requests.exceptions.RequestException as e:
            _logger.error(f"An error occurred: {e}")


class Config:
    @staticmethod
    def get_env_variable(name: str) -> str:
        """Get the environment variable or raise an exception."""
        try:
            return os.environ[name]
        except KeyError:
            raise RuntimeError(f"Environment variable {name} not set.")


@dataclass
class RequestData:
    requestId: str
    queryDate: str
    request: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "requestId": self.requestId,
            "queryDate": self.queryDate,
            "request": self.request,
        }


class AnnotationsService:
    def __init__(self):
        self.annotations_url = os.getenv(
            "URL_ANNOT",
            f"https://{_env}-api-interne.sacem.fr:443/octav/wesid/v1/securise/works/bulk/annotationStatic",
        )

    @_tracer.capture_method
    def _parse_paypoad(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Converts percentage strings in the 'timeRatio' field to floats.

        Args:
            data (Dict[str, Any]): Input dictionary with percentage strings.

        Returns:
            Dict[str, Any]: Modified dictionary with 'timeRatio' as floats.
        """
        for item in data.get("listIde12", []):
            if "timeRatio" in item:
                percentage_str = item["timeRatio"]
                # Remove the '%' character and convert to float
                item["timeRatio"] = float(percentage_str.strip("%")) / 100.0
        return data

    @_tracer.capture_method
    def _generate_payload(self, input_dict: Dict, terr_list: List[str]) -> List[Dict]:
        """Generates a Dict payload to be sent to the Annotations Service."""
        result = []
        for item in input_dict["listIde12"]:
            for territory in terr_list:
                new_item = {
                    "ide12": item["ide12"],
                    "territory": territory,
                    "situationDate": datetime.now().strftime("%d/%m/%Y"),
                }
                result.append(new_item)

        return result

    @_tracer.capture_method
    def _get_annotations(
        self, token: str, payload: Dict[str, Any], territories: List[str], url: str
    ) -> Dict[str, Any]:
        """Gets Remed annotations for a specific territory."""
        _payload = self._parse_paypoad(payload)
        request_dict = self._generate_payload(_payload, territories)
        headers = {
            "wessoauthcookie": f"{token}",
            "Content-Type": "application/json",
            "accept": "*/*",
        }
        data = RequestData(
            requestId=f"request-{int(datetime.now().strftime('%Y%m%d%H%M%S'))}",
            queryDate=f"{int(datetime.now().strftime('%Y%m%d'))}",
            request=request_dict,
        ).to_dict()
        response = requests.post(url, headers=headers, json=data)
        print(response.json())
        return response.json()


class AnnotationsFormatter:
    def __init__(self, data: Dict, payload: Dict):
        self.data = data
        self.payload = payload

    @_tracer.capture_method
    def _extract_data(self) -> List[Dict]:
        annotations = self.data.get("annotations", [])
        extracted_data = []
        for annotation in annotations:
            result = annotation.get("result", {})
            if result is None:
                extracted_data.append(
                    {
                        "ide12": annotation.get("ide12"),
                        "territory": annotation.get("territory"),
                        "dp": None,
                        "pai": None,
                        "ns": None,
                        "pr": None,
                    }
                )
            else:
                extracted_data.append(
                    {
                        "ide12": annotation.get("ide12"),
                        "territory": annotation.get("territory"),
                        "dp": result.get("dp"),
                        "pai": result.get("pai"),
                        "ns": result.get("ns"),
                        "pr": result.get("pr"),
                    }
                )

        return extracted_data

    @_tracer.capture_method
    def group_dataframe_by_ide12(self, df: pd.DataFrame) -> List[Dict]:
        """Groups the DataFrame by the 'ide12' column and transforms each row into a dictionary."""
        df["sum"] = df[["dp", "pai", "ns", "pr"]].sum(axis=1)
        df = df[["ide12", "territory", "dp", "pai", "ns", "pr"]]
        # Group DataFrame by IDE12
        grouped = df.groupby("ide12")
        result = []
        for ide12, group in grouped:
            group_dict = {
                "ide12": ide12,
                "results": group.drop(columns=["ide12"])
                .replace({float("nan"): None})
                .to_dict(orient="records"),
            }
            result.append(group_dict)

        return result

    @_tracer.capture_method
    def _transform_df_terr_to_dict(self, df: pd.DataFrame) -> dict:
        """Transforms the given DataFrame into a dictionary where the keys are the unique 'territory' values
        and the values are dictionaries with keys as column names and values as lists of corresponding values.
        """
        result = defaultdict(lambda: defaultdict(list))
        for _, row in df.iterrows():
            territory = row["territory"]
            for col in df.columns[1:]:
                result[territory][col].append(row[col])

        # Convert defaultdict to regular dict
        return {str(int(k)): dict(v) for k, v in result.items()}

    @_tracer.capture_method
    def _transform_df_ide12_to_dict(self, df: pd.DataFrame) -> List[Dict]:
        """Converts a pandas DataFrame into a list of dictionaries with the specified format."""
        result_list = []
        grouped = df.groupby("ide12")
        for ide12, group in grouped:
            result_dict = {
                "ide12": ide12,
                "results": group.drop(columns="ide12")
                .replace({float("nan"): None})
                .to_dict(orient="records"),
            }
            result_list.append(result_dict)

        return result_list

    @_tracer.capture_method
    def _transform_dict_to_df(self) -> pd.DataFrame:
        """Transforms the dictionary into a DataFrame."""
        annotations = self.data["annotations"]
        records = []
        for annotation in annotations:
            if annotation["errorInfo"] is not None:
                _logger.warning(
                    f"Warning: this error encountered while processing "
                    f"{annotation['territory']} for annotation['ide12']. Error: {annotation['errorInfo']}"
                )
                ide12 = annotation["ide12"]
                territory = annotation["territory"]
                # result = annotation['result']
                record = {
                    "ide12": ide12,
                    "territory": territory,
                    "dp": None,
                    "pai": None,
                    "ns": None,
                    "pr": None,
                }
                records.append(record)
            else:
                ide12 = annotation["ide12"]
                territory = annotation["territory"]
                result = annotation["result"]
                record = {
                    "ide12": ide12,
                    "territory": territory,
                    "dp": result["dp"],
                    "pai": result["pai"],
                    "ns": result["ns"],
                    "pr": result["pr"],
                }
                records.append(record)
        df = pd.DataFrame(records)
        df["ide12"] = df["ide12"].astype(str)
        df["territory"] = df["territory"].astype(str)
        return df

    @_tracer.capture_method
    def _get_time_ratios(self) -> pd.DataFrame:
        """Processes the input dictionary and returns the details."""
        list_ide12 = self.payload.get("listIde12", [])
        timeration_list = []
        for item in list_ide12:
            ide12 = item.get("ide12")
            time_ratio = item.get("timeRatio")
            timeration_list.append((ide12, time_ratio))
        df = pd.DataFrame(timeration_list, columns=["ide12", "timeRation"])
        df["timeRation"] = df["timeRation"].astype(float)
        df["timeRation"] = df["timeRation"] / 100  # Convert timeRatio into a percentage

        return df

    @_tracer.capture_method
    def _calculate_time_ratios(self, df: pd.DataFrame) -> pd.DataFrame:
        df["dp"] = df["dp"] * df["timeRation"] * 100
        df["pai"] = df["pai"] * df["timeRation"] * 100
        df["ns"] = df["ns"] * df["timeRation"] * 100
        df["pr"] = df["pr"] * df["timeRation"] * 100
        processed_df = df[
            ["ide12", "territory", "dp", "pai", "ns", "pr"]
        ]  # Select the required columns
        processed_df["ide12"] = processed_df["ide12"].astype(
            str
        )  # cast ide12 as string

        return processed_df

    @_tracer.capture_method
    def replace_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Replace specific values in the nested dictionary.
        """

        def process_results(
            results: List[Dict[str, Union[str, float]]],
        ) -> List[Dict[str, Union[str, float, None]]]:
            for result in results:
                if all(
                    result.get(key) == 0.0 for key in ["dp", "pai", "ns", "pr"]
                ) or all(result.get(key) == "NaN" for key in ["dp", "pai", "ns", "pr"]):
                    for key in ["dp", "pai", "ns", "pr"]:
                        result[key] = None
            return results

        if "container" in data:
            data["container"]["results"] = process_results(data["container"]["results"])

        if "records" in data:
            for record in data["records"]:
                record["results"] = process_results(record["results"])

        return data

    @_tracer.capture_method
    def process_data(self) -> Dict[str, Any]:
        """Processes the input dictionary."""
        extracted_data = self._extract_data()
        time_ratios_df = self._get_time_ratios()

        terr_df = pd.DataFrame(extracted_data)
        terr_df = terr_df[["territory", "ide12", "dp", "pai", "ns", "pr"]]
        terr_df["ide12"] = terr_df["ide12"].astype(str)

        time_ratios_df["ide12"] = time_ratios_df["ide12"].astype(str)
        merged_df = pd.merge(terr_df, time_ratios_df, on="ide12", how="inner")
        final_df = self._calculate_time_ratios(merged_df)
        final_df = final_df.drop(columns=["ide12"])

        # Group by 'territory' and sum the specified columns
        processed_df = final_df.groupby("territory", as_index=False).sum()
        processed_df["territory"] = processed_df["territory"].astype(str)
        processed_df = processed_df.replace({float("nan"): None})

        final_dict = {
            "container": {
                "containerId": self.payload.get("containerId"),
                "results": processed_df.to_dict(orient="records"),
            }
        }

        _logger.debug("Parsing annotations per IDE12 ... ")
        ide_df = self._transform_dict_to_df()
        ide_dict = self.group_dataframe_by_ide12(ide_df)
        final_dict["records"] = ide_dict
        # Replace incoherent API responses like NaN or null values
        final_dict = self.replace_values(final_dict)

        return json.dumps(final_dict)


@_tracer.capture_method
@_tracer.capture_lambda_handler
@_logger.inject_lambda_context
@_metrics.log_metrics(capture_cold_start_metric=True)
@_logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
def lambda_handler(event, context):
    """Lambda function handler that receives a trigger from API Gateway,
    sends a GET request with a header to Annotations API, and returns the data as a dictionary.
    """
    _secret_key = Config.get_env_variable("SECRETKEY")
    _url_wesid = Config.get_env_variable("URL_WESID")
    _url_annot = Config.get_env_variable("URL_ANNOT")
    _territories_list = Config.get_env_variable("TERRITORIES").split(", ")

    _octav_service = OctavService()
    _annotations_service = AnnotationsService()

    try:
        input_payload = event["body"]

        # Step 1: Retrieve the temporary token from Octav
        temp_token = _octav_service._get_token(
            url=_url_wesid, secret_key=_secret_key
        )  # noqa

        if not temp_token:
            raise ValueError("API_KEY Not valid or provided. Please Check !")

        # Step 2: Use the token to get data from AnnotationStatic
        annotations_data = _annotations_service._get_annotations(  # noqa
            temp_token, input_payload, _territories_list, _url_annot
        )

        if not annotations_data:
            raise ValueError(
                "No data retrieved from `AnnotationStatic`. Please Check !"
            )

        # Step 3: format received data by territory then by ide12
        _logger.debug("Format received data by territory then by ide12 ... ")
        extractor = AnnotationsFormatter(annotations_data, input_payload)
        final_dict = extractor.process_data()

        # Step 4: Log a +1 success metric into CW
        _logger.info(f"Data retrieved: {final_dict}")
        _metrics.add_metric(name="SuccessfulRequests", unit=MetricUnit.Count, value=1)
        _metrics.add_metadata(
            key="ContainerId", value=f"{input_payload['containerId']}"
        )

        return {"statusCode": 200, "body": final_dict}

    except Exception as e:
        # Step 4: Log a +1 failure metric into CW
        _metrics.add_metric(name="FailedRequests", unit=MetricUnit.Count, value=1)
        _metrics.add_metadata(
            key="ContainerId", value=f"{input_payload['containerId']}"
        )
        _logger.error(f"Error occurred: {str(e)}")

        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


if __name__ == "__main__":
    event = {
        "containerId": "500000308013",
        "listIde12": [
            {"ide12": "3759115911", "timeRatio": "6.96%"},
            {"ide12": "3770576911", "timeRatio": "7.70%"},
            {"ide12": "3770554611", "timeRatio": "6.23%"},
            {"ide12": "3770569011", "timeRatio": "7.31%"},
            {"ide12": "3770576211", "timeRatio": "6.95%"},
            {"ide12": "3720532111", "timeRatio": "6.17%"},
            {"ide12": "3770576311", "timeRatio": "7.15%"},
            {"ide12": "3771780611", "timeRatio": "6.01%"},
            {"ide12": "3770576711", "timeRatio": "8.09%"},
            {"ide12": "3402582011", "timeRatio": "6.49%"},
            {"ide12": "3770568711", "timeRatio": "7.05%"},
            {"ide12": "3724065811", "timeRatio": "7.70%"},
            {"ide12": "3770577111", "timeRatio": "6.69%"},
            {"ide12": "3770577011", "timeRatio": "9.50%"},
        ],
    }
    lambda_handler(event, "")
