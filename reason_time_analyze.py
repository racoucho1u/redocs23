import networkx as nx
from networkx import number_connected_components, get_node_attributes
import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.collections import PolyCollection
import plotly.express as px

node_file_path = 'bare_effects_filtered.jsonl'
edges_file_path = 'bare_edge_effects_filtered.jsonl'

data_nodes = pd.read_json('bare_effects_filtered.jsonl', lines=True)
data_edge = pd.read_json('bare_edge_effects_filtered.jsonl', lines=True)
normalized_data_edge = data_edge


# mauvais enchainement de login/logout _ connect/disconnect
def check_in_out(data_nodes, normalized_data_edge):

    reasons = ['DISCONNECT', 'HAS', 'EXECUTE', 'ENCODE', 'START', 'LOGOFF', 'USES', 'COMPRESS',
               'LOGON', 'INDICATE', 'CONTAINS', 'INITIAL', 'SEND', 'STOP', 'CREATE', 'RECEIVE', 'CONNECT']

    # reasons = ['DISCONNECT', 'EXECUTE', 'START', 'LOGOFF', 'USES',
    #            'LOGON', 'INDICATE', 'SEND', 'STOP', 'CREATE', 'RECEIVE', 'CONNECT']

    data = []

    reasonEdgesNotIn2023 = []

    for reason in reasons:

        edges = normalized_data_edge.query(f'reason=="{reason}"')

        print(f"Reason: {reason} -> Found {edges.shape[0]} occurences")

        timestamps = edges["timestamp"].values.tolist()

        in2021 = False
        for tp in timestamps:
            time_edge = tp['firstSeen']
            time_edge_dt = datetime.fromtimestamp(time_edge["seconds"])
            time_edge_dt += timedelta(
                microseconds=time_edge.get("nanos", 0) // 1000)
            if (time_edge_dt.year == 2021):
                in2021 = True
                break

        if (not in2021):
            reasonEdgesNotIn2023 += [reason]
            print("\t => All occurrences are not in 2021")
            continue

        print("="*50)

        for index, edge in edges.iterrows():

            time_edge = edge['timestamp']['firstSeen']
            time_edge_dt = datetime.fromtimestamp(time_edge["seconds"])
            time_edge_dt += timedelta(
                microseconds=time_edge.get("nanos", 0) // 1000)

            if (time_edge_dt.year != 2021):
                # print(f'Found "{reason}" edge is not in 2021')
                continue

            # print(time_edge_dt.isoformat())

            lower_bound_time = time_edge_dt - timedelta(seconds=5000)
            # print(lower_bound_time.isoformat())
            upper_bound_time = time_edge_dt + timedelta(seconds=5000)
            # print(upper_bound_time.isoformat())

            data += [(lower_bound_time, upper_bound_time, reason)]

    reasons = [reason for reason in reasons if reason not in reasonEdgesNotIn2023]

    cats = {reason: (index+1) for index, reason in enumerate(reasons)}
    colormapping = {reason: ("C" + str(index))
                    for index, reason in enumerate(reasons)}

    verts = []
    colors = []
    for d in data:
        v = [(mdates.date2num(d[0]), cats[d[2]]-.4),
             (mdates.date2num(d[0]), cats[d[2]]+.4),
             (mdates.date2num(d[1]), cats[d[2]]+.4),
             (mdates.date2num(d[1]), cats[d[2]]-.4),
             (mdates.date2num(d[0]), cats[d[2]]-.4)]
        verts.append(v)
        colors.append(colormapping[d[2]])

    bars = PolyCollection(verts, facecolors=colors)
    # bars = PolyCollection(verts)

    fig, ax = plt.subplots()
    ax.add_collection(bars)
    ax.autoscale()

    loc = mdates.DayLocator()
    ax.xaxis.set_major_locator(loc)
    # ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(loc))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y:%m:%d'))

    # Rotates and right-aligns the x labels so they don't crowd each other.
    for label in ax.get_xticklabels(which='major'):
        label.set(rotation=90, horizontalalignment='right')

    ax.set_yticks([(tick+1) for tick in range(len(reasons))])
    ax.set_yticklabels(reasons)
    plt.show()

    return print("done")


if __name__ == '__main__':
    check_in_out(data_nodes, normalized_data_edge)
