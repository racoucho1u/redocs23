# See PyCharm help at https://www.jetbrains.com/help/pycharm/
import json
import sys
import datetime


def adapt_graph(edges_input_file_path, edges_output_file_path):
    try:
        with open(edges_input_file_path, 'r') as ifile:
            with open(edges_output_file_path, 'w') as ofile:
                i = 0
                for line in ifile:
                    json_obj = json.loads(line)
                    if json_obj["_to"] == "b360991f-06cb-b76b-b726-81ee1093a781":
                        dt = datetime.datetime.fromtimestamp(json_obj["timestamp"]["firstSeen"]["seconds"])
                        print(dt.isoformat())
                        dt += datetime.timedelta(days=-3)
                        print(dt.isoformat())
                        json_obj["timestamp"]["firstSeen"]["seconds"] = int(dt.timestamp())
                        print(json_obj["timestamp"])
                        # break
                    if json_obj["_to"] == "1822220d-1120-a9f5-3b7c-18fdebc45535":
                        if json_obj["_from"] == "3548626e-bf41-5204-4cdc-b74c436b7072":
                            if json_obj["uuid"] == "24cfca59-714b-4774-8d39-d3062857f2a5":
                                dt = datetime.datetime.fromtimestamp(json_obj["timestamp"]["firstSeen"]["seconds"])
                                print(dt.isoformat())
                                dt += datetime.timedelta(minutes=-5)
                                print(dt.isoformat())
                                json_obj["timestamp"]["firstSeen"]["seconds"] = int(dt.timestamp())
                                print(json_obj)

                    if json_obj["_to"] == "f8fb79c1-467c-2ec7-f26f-bb2143371a41":
                        if json_obj["_from"] == "66a144e0-02ab-735b-878e-dfd4665cc96a":
                            json_obj["timestamp"]["firstSeen"] = json_obj["timestamp"]["timestamp"]
                            print(json_obj)

                    ofile.write(str.format("{0}\r", json.dumps(json_obj)))
                    """
                    i += 1
                    if i == 3:
                        break
                    """

    except FileNotFoundError:
        print("File not found.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python program.py <input_filtered_edge_jsonl_file> <output_edge_adapted_jsonl_file>")
    else:
        input_filtered_edge_jsonl_file = sys.argv[1]
        output_edge_adapted_jsonl_file = sys.argv[2]
        adapt_graph(input_filtered_edge_jsonl_file, output_edge_adapted_jsonl_file)
