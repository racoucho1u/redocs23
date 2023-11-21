import networkx as nx
from networkx import number_connected_components, get_node_attributes
import pandas as pd
from load_graph import load_graph
from datetime import datetime, timedelta

from graph import g

#node_file_path = 'bare_effects_filtered.jsonl'
#edges_file_path = 'bare_edge_effects_filtered.jsonl'

#data_nodes = pd.read_json('bare_effects_filtered.jsonl', lines=True)
#data_edge = pd.read_json('bare_edge_effects_filtered.jsonl', lines=True)
#normalized_data_edge = data_edge




# mauvais enchainement de login/logout _ connect/disconnect
def check_in_out(data_nodes, normalized_data_edge):

    # for a login edge, for the fromNode and toNode, check:
    #  - there is a logout edge from toNode to fromNode
    #  - we can not have "login" several times or "logout" several times
    #  - the first login's timestamp must be inferior to the logout's timestamp

    # for a connect edge, for the fromNode and toNode, check:
    #  - there is a disconnect edge "from toNode to fromNode" or "from fromNode to toNode"
    #  - we can not have "connect" several times or "disconnect" several times
    #  - the first connect's timestamp must be inferior to the disconnect's timestamp

    connect_edges = normalized_data_edge.query(f'reason == "CONNECT"')
    print("CONNECT")
    print(connect_edges[["_from", "_to", "reason", "entity"]])
    print("="*40)

    disconnect_edges = normalized_data_edge.query(f'reason == "DISCONNECT"')
    print("DISCONNECT")
    print(disconnect_edges[["_from", "_to", "reason", "entity"]])
    print("="*40)

    logon_edges = normalized_data_edge.query(f'reason == "LOGON"')
    print("LOGON")
    print(logon_edges[["_from", "_to", "reason", "entity"]])
    print("="*40)

    logoff_edges = normalized_data_edge.query(f'reason == "LOGOFF"')
    print("LOGOFF")
    print(logoff_edges[["_from", "_to", "reason", "entity"]])
    print("="*40)
    print("="*40)

    # Check we have a connect for each disconnect
    for index, edge in disconnect_edges.iterrows():
        res = connect_edges.query(f'(_from == "{edge["_to"]}" and _to == "{edge["_from"]}") or \
                                  (_from == "{edge["_from"]}" and _to == "{edge["_to"]}")')

        if (res.empty):
            print(
                f'No associated "DISCONNECT" edge for "CONNECT" edge {edge["_key"]}')
            continue

        time_edge = edge['timestamp']['firstSeen']
        time_edge_dt = datetime.fromtimestamp(time_edge["seconds"])
        time_edge_dt += timedelta(microseconds=time_edge.get("nanos", 0) // 1000)
        time_edge_dt.isoformat()

        for index, row in res.iterrows():
            time_edge_cmp = row['timestamp']['firstSeen']
            time_edge_dt_cmp = datetime.fromtimestamp(time_edge_cmp["seconds"])
            time_edge_dt_cmp += timedelta(
                microseconds=time_edge_cmp.get("nanos", 0) // 1000)
            time_edge_dt_cmp.isoformat()

            if (time_edge_dt_cmp <= time_edge_dt):
                print(
                    f'"DISCONNECT" edge {row["_key"]} is before "CONNECT" edge {edge["_key"]}')

    print("-"*70)

    valid_logon = []

    # Check we have a connect for each disconnect
    for index, edge in logoff_edges.iterrows():
        res = logon_edges.query(f'(_from == "{edge["_to"]}" and _to == "{edge["_from"]}") or \
                                (_from == "{edge["_from"]}" and _to == "{edge["_to"]}")')

        if (res.empty):
            print(
                f'No associated "LOGOFF" edge for "LOGON" edge {edge["_key"]}')
            continue

        time_edge = edge['timestamp']['firstSeen']
        time_edge_dt = datetime.fromtimestamp(time_edge["seconds"])
        time_edge_dt += timedelta(microseconds=time_edge.get("nanos", 0) // 1000)
        time_edge_dt.isoformat()

        for index, row in res.iterrows():
            time_edge_cmp = row['timestamp']['firstSeen']
            time_edge_dt_cmp = datetime.fromtimestamp(time_edge_cmp["seconds"])
            time_edge_dt_cmp += timedelta(
                microseconds=time_edge_cmp.get("nanos", 0) // 1000)
            time_edge_dt_cmp.isoformat()

            if (time_edge_dt_cmp <= time_edge_dt):
                print(
                    f'"LOGOFF" edge {row["_key"]} is before "LOGON" edge {edge["_key"]}')
            else:
                valid_logon += [row["_key"]]

    print("="*45)
    print('Invalid "LOGON" edge because not associated with a "LOGOFF" edge')
    invalid_logon = logon_edges.query(f'_key not in {valid_logon}')
    print(invalid_logon[["_from", "_to", "reason", "entity"]])

    return print("done")











def check_alternance(reasonA, reasonB, reasonA_can_fail, reasonB_can_fail=False):
    edgeA = [e in g.edges() if e.reason()==reasonA]
    edgeB = [e in g.edges() if e.reason()==reasonB]
    







def check_in_out():

    # for a login edge, for the fromNode and toNode, check:
    #  - there is a logout edge from toNode to fromNode
    #  - we can not have "login" several times or "logout" several times
    #  - the first login's timestamp must be inferior to the logout's timestamp

    # for a connect edge, for the fromNode and toNode, check:
    #  - there is a disconnect edge "from toNode to fromNode" or "from fromNode to toNode"
    #  - we can not have "connect" several times or "disconnect" several times
    #  - the first connect's timestamp must be inferior to the disconnect's timestamp


    connect_edges = [e in g.edges() if e.reason()=="CONNECT"]
    #connect_edges = normalized_data_edge.query(f'reason == "CONNECT"')
    #print("CONNECT")
    #print(connect_edges[["_from", "_to", "reason", "entity"]])
    #print("="*40)

    disconnect_edges = [e in g.edges() if e.reason()=="DISCONNECT"]
    #disconnect_edges = normalized_data_edge.query(f'reason == "DISCONNECT"')
    #print("DISCONNECT")
    #print(disconnect_edges[["_from", "_to", "reason", "entity"]])
    #print("="*40)

    logon_edges = [e in g.edges() if e.reason()=="LOGON"]
    #logon_edges = normalized_data_edge.query(f'reason == "LOGON"')
    #print("LOGON")
    #print(logon_edges[["_from", "_to", "reason", "entity"]])
    #print("="*40)

    logoff_edges = [e in g.edges() if e.reason()=="LOGOFF"]
    #logoff_edges = normalized_data_edge.query(f'reason == "LOGOFF"')
    #print("LOGOFF")
    #print(logoff_edges[["_from", "_to", "reason", "entity"]])
    #print("="*40)
    #print("="*40)


    # Check we have a connect for each disconnect
    for de in disconnect_edges:
        res = [ce for ce in connect_edges if ce.same_nodes(de)]
        #res = connect_edges.query(f'(_from == "{edge["_to"]}" and _to == "{edge["_from"]}") or (_from == "{edge["_from"]}" and _to == "{edge["_to"]}")')




        if (res.empty):
            print(
                f'No associated "DISCONNECT" edge for "CONNECT" edge {edge["_key"]}')
            continue

        time_edge = edge['timestamp']['firstSeen']
        time_edge_dt = datetime.fromtimestamp(time_edge["seconds"])
        time_edge_dt += timedelta(microseconds=time_edge.get("nanos", 0) // 1000)
        time_edge_dt.isoformat()

        for index, row in res.iterrows():
            time_edge_cmp = row['timestamp']['firstSeen']
            time_edge_dt_cmp = datetime.fromtimestamp(time_edge_cmp["seconds"])
            time_edge_dt_cmp += timedelta(
                microseconds=time_edge_cmp.get("nanos", 0) // 1000)
            time_edge_dt_cmp.isoformat()

            if (time_edge_dt_cmp <= time_edge_dt):
                print(
                    f'"DISCONNECT" edge {row["_key"]} is before "CONNECT" edge {edge["_key"]}')

    print("-"*70)

    valid_logon = []

    # Check we have a connect for each disconnect
    for index, edge in logoff_edges.iterrows():
        res = logon_edges.query(f'(_from == "{edge["_to"]}" and _to == "{edge["_from"]}") or \
                                (_from == "{edge["_from"]}" and _to == "{edge["_to"]}")')

        if (res.empty):
            print(
                f'No associated "LOGOFF" edge for "LOGON" edge {edge["_key"]}')
            continue

        time_edge = edge['timestamp']['firstSeen']
        time_edge_dt = datetime.fromtimestamp(time_edge["seconds"])
        time_edge_dt += timedelta(microseconds=time_edge.get("nanos", 0) // 1000)
        time_edge_dt.isoformat()

        for index, row in res.iterrows():
            time_edge_cmp = row['timestamp']['firstSeen']
            time_edge_dt_cmp = datetime.fromtimestamp(time_edge_cmp["seconds"])
            time_edge_dt_cmp += timedelta(
                microseconds=time_edge_cmp.get("nanos", 0) // 1000)
            time_edge_dt_cmp.isoformat()

            if (time_edge_dt_cmp <= time_edge_dt):
                print(
                    f'"LOGOFF" edge {row["_key"]} is before "LOGON" edge {edge["_key"]}')
            else:
                valid_logon += [row["_key"]]

    print("="*45)
    print('Invalid "LOGON" edge because not associated with a "LOGOFF" edge')
    invalid_logon = logon_edges.query(f'_key not in {valid_logon}')
    print(invalid_logon[["_from", "_to", "reason", "entity"]])

    return print("done")
