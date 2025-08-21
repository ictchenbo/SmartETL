"""
 OpenAPI reader/writer
 see https://www.openapis.org/
"""
import os
import re
from typing import Any

from smartetl.processor import Flat
from smartetl.processor.aggs import ReduceBase
from smartetl.util.dates import current_ts


type_mapping = {
    "integer": "int",
    "number": "float",
    "boolean": "bool"
}
type_mapping_reverse = {v: k for k, v in type_mapping.items()}


class SchemaTransform():
    def __init__(self, schemas: dict):
        self.schemas = schemas

    def resolve(self, class_name: str):
        key = class_name[len("#/components/schemas/"):]
        return self.schemas[key]

    def from_any_of(self, any_of: list):
        one = any_of[0]
        return self.from_schema(one)

    def from_schema(self, schema: dict):
        if "anyOf" in schema:
            return self.from_any_of(schema["anyOf"])
        if "$ref" in schema:
            return self.resolve(schema["$ref"])

        return schema

    def transform_object(self, schema: dict):
        ret = {}
        required = schema.get("required") or []
        for name, _prop in schema.get("properties", {}).items():
            prop = self.transform_schema(_prop)
            if name in required:
                prop["__required"] = True
            # ret[name] = {
            #     "__type": prop.get("type"),
            #     "__required": name in required
            # }
            ret[name] = prop

        if "additionalProperties" in schema:
            addi = schema["additionalProperties"]
            if "$ref" in addi or "anyOf" in addi:
                addi = self.transform_schema(addi)
                ret.update(addi)
            else:
                for key, subtype in addi.items():
                    if isinstance(subtype, str):
                        _value = {
                            "__type": type_mapping.get(subtype) or subtype
                        }
                    else:
                        _value = self.transform_schema(subtype)
                    ret[key] = _value
        return ret

    def transform_array(self, schema: dict):
        ret = {}
        ret["__items"] = self.transform_schema(schema.get("items"))
        return ret

    def transform_schema(self, schema: dict):
        schema = self.from_schema(schema)

        _type = schema.get("type") or "string"

        if _type == "object":
            ret = self.transform_object(schema)
        elif _type == "array":
            ret = self.transform_array(schema)
        else:
            ret = {}

        desc = schema.get("title") or ret.get("__description")
        if desc:
            ret["__description"] = desc

        _def = schema.get("default")
        if _def:
            ret["__default"] = _def

        if "maximum" in schema:
            ret["__max"] = schema["maximum"]

        ret["__type"] = type_mapping.get(_type) or _type

        if schema.get("required"):
            ret["__required"] = True

        return ret

    def transform_parameter(self, parameter: dict):
        # print(parameter)
        ret = self.transform_schema(parameter.get("schema"))

        desc = parameter.get("description") or ret.get("__description")
        if desc:
            ret["__description"] = desc

        if parameter.get("required"):
            ret["__required"] = True

        return ret

    def parse_parameters(self, parameters: list):
        # 4 types of holders
        path, query, header = {}, {}, {}
        ret = dict(path=path, query=query, header=header)
        for parameter in parameters:
            name = parameter["name"]
            _in = parameter.get("in", "query")
            ret[_in][name] = self.transform_parameter(parameter)
        # remove empty holder
        for key in ["path", "query", "header"]:
            if len(ret[key]) == 0:
                ret.pop(key)

        return ret

    def parse_body(self, body: dict):
        ret_body = {}
        required = body.get('required')
        for _type, _value in body.get('content', {}).items():
            ret_body = self.transform_parameter(_value)
            ret_body["__contenttype"] = _type
            break

        if required:
            ret_body["__required"] = True

        return ret_body

    def parse_response(self, responses: dict):
        ok_response = responses.get("200") or {}
        content = ok_response.get("content") or {}
        for content_type, body in content.items():
            result = self.transform_parameter(body)
            result["__contenttype"] = content_type
            return {
                "body": result
            }

        return {}


def read(openapi_doc: dict) -> list:
    """读取OpenAPI规范 转换成内部模型规范"""
    models = []
    info = openapi_doc.get("info") or {}
    title = info.get("title")
    info_desc = info.get("description")
    contact = info.get("contact")
    id_prefix = re.sub(r'\W+', '_', title.lower())
    version = info.get("version")

    tags = openapi_doc.get("tags") or []
    simple_tags = [tag["name"] for tag in tags]

    timestamp = current_ts()

    servers = openapi_doc.get("servers") or []
    if servers:
        server = servers[0]["url"]
    else:
        server = os.environ.get("api.server")

    schemas = openapi_doc.get("components", {}).get("schemas", {})

    t = SchemaTransform(schemas)

    for path, method_service in openapi_doc.get("paths", {}).items():
        for method, _service in method_service.items():

            input_args = t.parse_parameters(_service.get("parameters") or [])
            body = t.parse_body(_service.get("requestBody", {}))
            if body:
                input_args["body"] = body

            service = {
                "address": server,
                "path": path,
                "method": method,
                "input_args": input_args,
                "output_args": t.parse_response(_service.get("responses") or {})
            }

            name = _service.get("summary") or f"{title}:{path}:{method}"

            tags = list(simple_tags)
            tags.extend(_service.get("tags", []))

            model = {
                "_id": _service.get("operationId") or f"{id_prefix}_{path}_{method}",
                "name": name,
                "description": _service.get("description") or f"service: {name}, method: {method}, path: {path}",
                "tags": list(set(tags)),
                "version": version,
                "service": service,
                "comment": info_desc,
                "source": "web",
                "owner": contact.get("name"),
                "created_time": timestamp,
                "modified_time": timestamp
            }

            models.append(model)

    return models


key_map = {
    "title": "__description",
    "maximum": "__max",
    "default": "__default"
}


def as_schema(field_def: dict):
    _type = field_def.get("__type") or "string"

    schema = {
        "type": type_mapping_reverse.get(_type) or _type,
        "required": field_def.get("__required") is True
    }

    for k1, k2 in key_map.items():
        if k2 in field_def:
            schema[k1] = field_def[k2]

    if _type == "object":
        properties = {}
        for k, val_def in field_def.items():
            if not k.startswith('__'):
                properties[k] = as_schema(val_def)

        schema["properties"] = properties
    elif _type == "array":
        schema["items"] = as_schema(field_def.get("__items") or {})

    return schema


def as_parameters(**kwargs):
    ret = []
    for _in, params in kwargs.items():
        for key, value in params.items():
            parameter = {
                "name": key,
                "in": _in,
                "description": value.get("__description"),
                "schema": as_schema(value)
            }
            ret.append(parameter)

    return ret


def write(model_list: list) -> dict:
    """将本系统的API结构转换成OpenAPI"""
    # print("total APIs:", len(model_list))
    main_model = model_list[0]

    paths = {}
    for model in model_list:
        service = model["service"]
        input_args = service.get("input_args") or {}
        operation_obj = {
            "operationId": model.get("_id"),
            "description": model.get("description"),
            "parameters": as_parameters(path=input_args.get("path") or {}, query=input_args.get("query") or {})
        }
        if "body" in input_args:
            body = input_args.get("body")
            content_type = body.get("__contenttype", "application/json")
            operation_obj["requestBody"] = {
                "content": {
                    content_type: {
                        "schema": as_schema(body)
                    }
                }
            }
        output_args = service.get("output_args") or {}
        body = output_args.get("body")
        content_type = body.get("__contenttype", "application/json")
        operation_obj["responses"] = {
            "200": {
                "content": {
                    content_type: {
                        "schema": as_schema(body)
                    }
                }
            }
        }

        paths[service["path"]] = {
            service.get("method", "get"): operation_obj
        }

    return {
        "openapi": "3.1.0",
        "info": {
            "title": main_model["name"],
            "version": main_model["version"],
        },
        "servers": [{"url": main_model["service"]["address"]}],
        "paths": paths,
        "tags": [{"name": tag} for tag in main_model.get("tags", [])]
    }
