import yaml


class SchemaLoader:
    def __init__(self, schema_file_path: str):
        self.schema_file_path = schema_file_path

    def load_schema(self) -> dict:
        with open(self.schema_file_path, "r") as file:
            schema = yaml.safe_load(file)
        return schema
