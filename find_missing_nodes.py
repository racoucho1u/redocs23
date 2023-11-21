import pandas as pd
import numpy as np

def get_missing_nodes(node_file_path, edges_file_path):

    print('Loading the data...')
    data_nodes = pd.read_json(f'{node_file_path}', lines=True)
    # put the created attribute to a time attribute
    data_edge = pd.read_json(f'{edges_file_path}', lines=True)
    print('Data is successfully loaded...')

    uniques_nodes = set(data_nodes['_key'].unique().tolist())
    uniques_nodes_from_edges = set(np.concatenate([data_edge['_to'], data_edge['_from']]).tolist())

    missing_nodes = set.difference(uniques_nodes_from_edges, uniques_nodes)
    print('The size of the missing nodes is: ', len(missing_nodes))
    return missing_nodes


if __name__ == "__main__":
    node_file_path = 'data/bare_effects_filtered.jsonl'
    edges_file_path = 'data/bare_edge_effects_filtered.jsonl'
    missing_nodes = get_missing_nodes(node_file_path, edges_file_path)
    with open("missing_nodes.txt", "w") as output:
        output.write(str(missing_nodes))
        
    print('The missing nodes are saved in the file: missing_nodes.txt')