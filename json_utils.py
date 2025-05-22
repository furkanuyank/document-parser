import json
import re
from dicttoxml import dicttoxml


def extract_json_from_text(text):
    try:
        print(text)
        json_match = re.search(r"\{.*\}", text, re.DOTALL)
        print(json_match)
        if json_match:
            json_data = json.loads(json_match.group())
            return json_data
        else:
            raise ValueError("Geçerli bir JSON bulunamadı.")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON ayrıştırma hatası: {e}")


def validate_keys(result, schema, type):
    if type == "field":
        fields = [field.split(":")[0].strip() for field in schema.split(",")]

        for field in fields:
            if field not in result:
                return False
        return True

    elif type == "schema":
        try:
            schema = json.loads(schema)
        except:
            return False
        return _validate_schema_keys(result, schema)

    return True


def _validate_schema_keys(result, schema):
    for key in schema.keys():
        if key not in result:
            return False
        elif isinstance(schema[key], dict) and isinstance(result[key], dict):
            if not _validate_schema_keys(result[key], schema[key]):
                return False
        elif isinstance(schema[key], list) and isinstance(result[key], list):
            if len(schema[key]) > 0 and isinstance(schema[key][0], dict):
                for item in result[key]:
                    if not _validate_schema_keys(item, schema[key][0]):
                        return False
    for key in result.keys():
        if key not in schema:
            return False
    return True


def validate_types(result, schema, type):
    if type == "field":
        fields = {field.split(":")[0].strip(): field.split(":")[1].strip() for field in schema.split(",")}

        for key, expected_type in fields.items():
            if key not in result:
                continue
            value = result[key]
            if value is None:
                continue
            if expected_type == "string" and not isinstance(value, str):
                return False
            elif expected_type == "number" and not isinstance(value, (int, float)):
                return False
            elif expected_type == "boolean" and not isinstance(value, bool):
                return False
        return True

    elif type == "schema":
        try:
            schema = json.loads(schema)
        except:
            return False
        return _validate_schema_types(result, schema)

    return False


def _validate_schema_types(result, schema):
    for key, expected_type in schema.items():
        if key not in result:
            continue
        value = result[key]
        if isinstance(expected_type, dict) and isinstance(value, dict):
            if not _validate_schema_types(value, expected_type):
                return False
        elif isinstance(expected_type, list) and isinstance(value, list):
            if len(expected_type) > 0 and isinstance(expected_type[0], dict):
                for item in value:
                    if not _validate_schema_types(item, expected_type[0]):
                        return False
        else:
            if value is None:
                continue
            if expected_type == "string" and not isinstance(value, str):
                return False
            elif expected_type == "number" and not isinstance(value, (int, float)):
                return False
            elif expected_type == "boolean" and not isinstance(value, bool):
                return False
    return True


def validate(result, schema, type):
    is_keys_valid = validate_keys(result, schema, type)
    is_types_valid = validate_types(result, schema, type)
    return is_keys_valid, is_types_valid


# TODO: aynı keyli farklı değerleri birleştirirken ne yapıalcağına karar ver
def merge_json_list(json_list):
    if not json_list:
        raise ValueError("JSON listesi boş olamaz.")

    if len(json_list) == 1:
        return json_list[0]

    merged_json = json_list[0]
    for json_obj in json_list[1:]:
        merged_json = merge_deneysel(merged_json, json_obj)

    return merged_json


def merge_jsons(json1, json2):
    merged = {}

    all_keys = set(json1.keys()).union(set(json2.keys()))

    for key in all_keys:
        value1 = json1.get(key)
        value2 = json2.get(key)

        if key in json1 and key in json2:
            if isinstance(value1, list) and isinstance(value2, list):
                merged[key] = list(set(value1 + value2))
            elif isinstance(value1, list):
                merged[key] = list(set(value1 + [value2]))
            elif isinstance(value2, list):
                merged[key] = list(set(value2 + [value1]))
            elif not isinstance(value1, list) and not isinstance(value2, list):
                if value1 == value2:
                    merged[key] = value1
                else:
                    merged[key] = [value1, value2]
        elif key in json1:
            merged[key] = value1
        elif key in json2:
            merged[key] = value2

    return merged


# valueler sıralı, keyler sıralı, null'lar alınmıyor
# dizi olmayan aynı keyleri nasıl birleştirileceğine bakılmalı
from collections import OrderedDict


def merge_deneysel(json1, json2):
    merged = OrderedDict()
    all_keys = list(json1.keys()) + [key for key in json2.keys() if key not in json1]

    for key in all_keys:
        value1 = json1.get(key)
        value2 = json2.get(key)

        if value1 is None and value2 is None:
            merged[key] = None
            continue
        if value1 is None:
            merged[key] = value2
            continue
        if value2 is None:
            merged[key] = value1
            continue

        if isinstance(value1, list) and isinstance(value2, list):
            merged[key] = []
            for item in value1 + value2:
                if item is not None and item not in merged[key]:
                    merged[key].append(item)
        elif isinstance(value1, list):
            merged[key] = [item for item in value1 if item is not None]
            if value2 not in merged[key] and value2 is not None:
                merged[key].append(value2)
        elif isinstance(value2, list):
            merged[key] = [item for item in value2 if item is not None]
            if value1 not in merged[key] and value1 is not None:
                merged[key].append(value1)
        elif not isinstance(value1, list) and not isinstance(value2, list):
            if value1 == value2:
                merged[key] = value1
            else:
                merged[key] = [value1, value2]

    return merged


def json_to_xml(json):
    xml_bytes = dicttoxml(json, custom_root="root", attr_type=False)
    xml_output = xml_bytes.decode("utf-8")
    return xml_output