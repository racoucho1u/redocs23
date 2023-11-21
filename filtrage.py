# See PyCharm help at https://www.jetbrains.com/help/pycharm/
import json
import sys


def filter_edge_jsonl_file(file_path, output_file_path):
    try:
        with open(file_path, 'r') as ifile:
            with open(output_file_path, 'w') as ofile:
                out_json_obj = {}
                for line in ifile:
                    json_obj = json.loads(line)
                    # Process the json_obj as per your requirements
                    # Access specific fields using json_obj['key']
                    out_json_obj["_key"] = json_obj["_key"]
                    # remove "bare_effects" from this objects
                    out_json_obj["_from"] = json_obj["cause"]
                    out_json_obj["_to"] = json_obj["consequence"]

                    out_json_obj["timestamp"] = json_obj["timestamp"]
                    out_json_obj["reason"] = json_obj["reason"]
                    out_json_obj["entity"] = json_obj["entity"]
                    out_json_obj["uuid"] = json_obj["uuid"]

                    # print(json_obj)
                    # print(out_json_obj)
                    ofile.write(json.dumps(out_json_obj) + "\r")
                    # break
    except FileNotFoundError:
        print("File not found.")


def filter_node_jsonl_file(file_path, output_file_path):
    try:
        with open(file_path, 'r') as ifile:
            with open(output_file_path, 'w') as ofile:
                out_json_obj = {}
                for line in ifile:
                    json_obj = json.loads(line)
                    # Process the json_obj as per your requirements
                    # Access specific fields using json_obj['key']
                    out_json_obj["_key"] = json_obj["_key"]
                    out_json_obj["created"] = json_obj["created"]
                    out_json_obj["length"] = json_obj["length"]
                    out_json_obj["name"] = json_obj["name"]
                    out_json_obj["timestamp"] = json_obj["timestamp"]
                    out_json_obj["type"] = json_obj["type"]
                    out_json_obj["uuid"] = json_obj["uuid"]

                    # print(json_obj)
                    # print(out_json_obj)
                    ofile.write(json.dumps(out_json_obj) + "\r")
                    # break
    except FileNotFoundError:
        print("File not found.")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python program.py <input_edge_jsonl_file> <output_edge_jsonl_file> <input_node_jsonl_file> "
              "<output_node_jsonl_file>")
    else:
        input_edge_jsonl_file = sys.argv[1]
        output_edge_jsonl_file = sys.argv[2]
        input_node_jsonl_file = sys.argv[3]
        output_node_jsonl_file = sys.argv[4]

        filter_edge_jsonl_file(input_edge_jsonl_file, output_edge_jsonl_file)
        filter_node_jsonl_file(input_node_jsonl_file, output_node_jsonl_file)
