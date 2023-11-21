import networkx as nx
from networkx import number_connected_components, get_node_attributes
import pandas as pd
from load_graph import load_graph

node_file_path = 'bare_effects_filtered.jsonl'
edges_file_path = 'bare_edge_effects_filtered.jsonl'

data_nodes = pd.read_json('bare_effects_filtered.jsonl', lines=True)
data_edge = pd.read_json('bare_edge_effects_filtered.jsonl', lines=True)
# iterate over the the rows of the dataframe


def check_no_timestamp(data_nodes, data_edge):

    for index, row in data_edge.iterrows():

        if ('timestamp' not in row.keys()):
            print(f'no timestamp in edge "{row}"')
        
        elif(row['timestamp'] == {}):
            print(f'empty timestamp in edge "{row}"')

    for index, row in data_nodes.iterrows():

        if ('timestamp' not in row.keys()):
            print(f'no timestamp in node "{row}"')

        elif(row['timestamp'] == {}):
            print(f'empty timestamp in node "{row}"')

        elif(row['timestamp']['timestamp'] == {}):
            print(f'empty timestamp.timestamp in node "{row}"')

    return print("done")


if __name__ == '__main__':
    check_no_timestamp(data_nodes, data_edge)
