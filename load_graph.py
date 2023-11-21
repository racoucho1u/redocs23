# See PyCharm help at https://www.jetbrains.com/help/pycharm/
import json
import sys
import networkx as nx

def load_graph(node_file_path, edges_file_path):
    try:
        nodes = []
        edges = []
        with open(node_file_path, 'r') as file:
            i = 0
            for line in file:
                json_obj = json.loads(line)
                nodes.append(json_obj)
                """
                i += 1
                if i == 3:
                    break
                """

        with open(edges_file_path, 'r') as file:
            i = 0
            for line in file:
                json_obj = json.loads(line)
                edges.append(json_obj)
                """
                i += 1
                if i == 3:
                    break
                """
        graph = nx.Graph()

        # Add nodes to the graph
        for node in nodes:
            graph.add_node(node["_key"], attr_dict=node)

        # Add edges to the graph
        for edge in edges:
            graph.add_edge(edge["_from"], edge["_to"], attr_dict=edge)

        return graph

    except FileNotFoundError:
        print("File not found.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python program.py <input_node_jsonl_file> <input_edge_adapted_jsonl_file>")
    else:
        input_node_jsonl_file = sys.argv[1]
        input_edge_adapted_jsonl_file = sys.argv[2]
        graph = load_graph(input_node_jsonl_file, input_edge_adapted_jsonl_file)
