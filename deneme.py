import json
from json_utils import merge_json_list, merge_jsons,json_to_xml, merge_deneysel
from xml.dom.minidom import parseString

def prettify_xml(xml_string):
    dom = parseString(xml_string)
    return dom.toprettyxml(indent="  ")


json_list = [
    {
        "name": None,
        "age": 30,
        "hobbies": ["reading", "traveling"],
        "location": "New York"
    },
    {
        "name": None,
        "age": 30,
        "hobbies": ["traveling", "sports",None],
        "location": None,
        "profession": "Engineer"
    },
    {
        "name": None,
        "age": 25,
        "hobbies": ["music", "sports"],
        "location": "Chicago",
        "profession": "Engineer"
    }
]


merged_json = merge_json_list(json_list)


output_json = json.dumps(merged_json, indent=2, ensure_ascii=False)
output_xml = json_to_xml(merged_json)

print("----------JSON----------")
print(output_json)
print("----------XML----------")
print(prettify_xml(output_xml))


