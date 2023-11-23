from collections import OrderedDict
import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.collections import PolyCollection
import plotly.express as px
from graph import g, Node#, ARROW_CHAR
from typing import Dict, Any, List, Tuple
from pprint import pprint
import json

node_file_path = 'bare_effects_filtered.jsonl'
edges_file_path = 'bare_edge_effects_filtered.jsonl'

data_nodes = pd.read_json('bare_effects_filtered.jsonl', lines=True)
data_edge = pd.read_json('bare_edge_effects_filtered.jsonl', lines=True)
normalized_data_edge = data_edge


"""
The "Iterative Partitioning Log Mining" (IPLoM) algorithm is a state-of-the-art technique for log parsing and clustering. While there are no direct mentions of algorithms similar to IPLoM in the provided search results, the field of log mining and clustering includes related techniques such as Lopper, a method for online log pattern mining based on hybrid clustering tree
1
, and a novel clustering algorithm for log file data sets presented in "Mining event logs with SLCT"
1
. These techniques, while not identical to IPLoM, share the common goal of effectively analyzing and clustering log data.
"""


def compute_all_sequence_number(depth: int, studied_node_types: List = None, nb_nodes: int = None) -> List:
    # obtient les enchainements d'action (edge.reason) les plus fréquents sur n edges consécutifs

    def compute_sequences_frequences(reason_sequences: List):
        sequences_frequencies = {}
        for sequence_index, reason_sequence in enumerate(reason_sequences):
            string_reason_sequence = ",".join(reason_sequence)
            if (not string_reason_sequence in list(sequences_frequencies.keys())):
                sequences_frequencies[string_reason_sequence] = 0
            sequences_frequencies[string_reason_sequence] += 1
        return sequences_frequencies

    reason_sequences: List = []

    def compute_all_sequence_number_aux(root: Node, reason_sequence: List, n: int) -> Any:
        if (n == 0):
            nonlocal reason_sequences
            reason_sequences += [reason_sequence + [root.type]]
        else:
            # get reasons in neighbouring nodes
            next_edges = g.edges_from(root.uuid)
            for next_edge in next_edges:
                next_node = next_edge.end()
                compute_all_sequence_number_aux(
                    next_node, reason_sequence + [root.type, next_edge.reason], n-1)

    sequence_frequence_per_node_type = {}

    node_index = 0
    for node in g.nodes():
        if (studied_node_types is None or (studied_node_types is not None and node.type in studied_node_types)):

            if (nb_nodes != None):
                if (node_index > nb_nodes):
                    break
            compute_all_sequence_number_aux(node, [], depth)
            if (len(reason_sequences) == 0):
                continue
            print(f'Computes {depth} action sequences from {node.uuid} node number {node_index}')
            # print(reason_sequences)
            sequences_num_for_curr_node = compute_sequences_frequences(
                reason_sequences)

            if (not node.type in list(sequence_frequence_per_node_type.keys())):
                sequence_frequence_per_node_type[node.type] = {}

            for string_sequence, num in sequences_num_for_curr_node.items():
                if (not string_sequence in list(sequence_frequence_per_node_type[node.type].keys())):
                    sequence_frequence_per_node_type[node.type][string_sequence] = 0
                sequence_frequence_per_node_type[node.type][string_sequence] += num

            # print(sequence_frequence_per_node_type)
            # print("-"*50)
            reason_sequences = []
            node_index += 1

    print("\n\n")

    sorted_sequence_frequence_per_node_type = {}
    for node_type, sequences_numbers in sequence_frequence_per_node_type.items():
        d = [(node_type, sequence_number)
             for node_type, sequence_number in sequences_numbers.items()]
        d.sort(key=lambda tup: tup[1])
        d.reverse()
        d = dict((OrderedDict(d)))
        print(
            "-"*22, f' Number of {depth} action sequences starting from all {node_type} nodes', "-"*22)
        for string_sequence, number in d.items():
            print(f'  {string_sequence.replace(",", ">")}: {number}')
        print("-"*50)
        sorted_sequence_frequence_per_node_type[node_type] = d

    json.dump(sorted_sequence_frequence_per_node_type, open(f"{depth}_sequence_number_per_node_type.json", "w+"), indent=4)

    return sorted_sequence_frequence_per_node_type


if __name__ == '__main__':

    compute_all_sequence_number(depth=1) # studied_node_types=["system.host", "system.workgroup", "system.process"])
