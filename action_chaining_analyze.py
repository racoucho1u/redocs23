import networkx as nx
from networkx import number_connected_components, get_node_attributes
import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.collections import PolyCollection
import plotly.express as px
from graph import g, Node
from typing import Dict, Any, List, Tuple

node_file_path = 'bare_effects_filtered.jsonl'
edges_file_path = 'bare_edge_effects_filtered.jsonl'

data_nodes = pd.read_json('bare_effects_filtered.jsonl', lines=True)
data_edge = pd.read_json('bare_edge_effects_filtered.jsonl', lines=True)
normalized_data_edge = data_edge


def get_frequent_chaining(depth: int, nb_nodes: int) -> List:
    # obtient les enchainements d'action (edge.reason) les plus fréquents sur n edges consécutifs

    reason_sequences: List = []

    def get_frequent_chaining_aux(root: Node, reason_sequence: List, n: int) -> Any:

        if (n == 0):
            nonlocal reason_sequences
            reason_sequences += [reason_sequence]
            return reason_sequence
        else:
            # get reasons in neighbouring nodes
            next_edges = g.edges_from(node.uuid)

            for next_edge in next_edges:

                next_node = next_edge.end()
                # reason_sequence = get_frequent_chaining_aux(
                #     next_node, reason_sequence + [(root.uuid, root.type, next_edge.reason, next_node.uuid, next_node.type)], n-1)
                reason_sequence = get_frequent_chaining_aux(
                    next_node, reason_sequence + [(next_edge.reason)], n-1)
        return []

    node_index = 0
    for node in g.nodes():
        # , "system.workgroup", "system.process"]):
        if (node.type in ["system.host"]):
            if (node_index > nb_nodes):
                break
            get_frequent_chaining_aux(node, [], depth)
            if (len(reason_sequences) == 0):
                continue
            print("="*22, f' {node_index} - {node.type} ', "="*22)
            print(reason_sequences)
            print("-"*50)
            reason_sequences = []
            node_index += 1

    return []


if __name__ == '__main__':
    get_frequent_chaining(depth=3, nb_nodes=3)
