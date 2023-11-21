import json
import csv
from datetime import datetime


filepath_nodes = "bare_effects_filtered.jsonl"
filepath_edges = "bare_edge_effects_filtered.jsonl"


output_nodes = "nodes.csv"
output_edges = "edges.csv"

nodes = {}


def timestamp_lt(timestamp_a, timestamp_b):
    
    """
    timestamp are dict with structure :
        "seconds" : int,
        "nanos" : int
    """
    second_a = timestamp_a["seconds"]
    second_b = timestamp_b["seconds"]
    
    nano_a = 0
    nano_b = 0
    
    if "nanos" in timestamp_a:
        nano_a = timestamp_a["nanos"]
    
    if "nanos" in timestamp_b:
        nano_b = timestamp_b["nanos"]
        
    return (second_a < second_b) or (second_a == second_b and nano_a < nano_b)


with open(filepath_nodes,"r") as f:
    
    for line in f.readlines():
        
        payload = json.loads(line)
        
        key = payload["_key"]
        created = payload["created"]
        length = payload["length"]
        name = payload["name"]
        timestamps = payload["timestamp"]
        ptype = payload["type"]
        uuid = payload["uuid"]
        
        tseen = timestamps["seen"]
        ttimestamp = timestamps["timestamp"]
        tlast = timestamps["lastSeen"]
        tfirst = timestamps["firstSeen"]
        
        tt_seconds = ttimestamp["seconds"]
        tt_nanos = 0
        
        tl_seconds = tlast["seconds"]
        tl_nanos = 0
        
        tf_seconds = tfirst["seconds"]
        tf_nanos = 0
        
        if "nanos" in ttimestamp:
            tt_nanos = ttimestamp["nanos"]
        
        if "nanos" in tlast:
            tl_nanos = tlast["nanos"]
        
        if "nanos" in tfirst:
            tf_nanos = tfirst["nanos"]
            
        row = {
            "key":key,
            "created":created,
            "length":length,
            "name":name,
            "type":ptype,
            "uuid":uuid,
            "ttseconds":tt_seconds,
            "tt_nanos": tt_nanos,
            "tf_seconds" : tf_seconds,
            "tf_nanos" : tf_nanos,
            "tl_seconds" : tl_seconds,
            "tl_nanos" : tl_nanos,
            "tseen" : tseen
        }
        
        nodes[key] = row
        


with open(filepath_edges,"r") as f:
    
    for line in f.readlines():
        
        payload = json.loads(line)
        
        key = payload["_key"]
        pfrom = payload["_from"]
        pto = payload["_to"]
        uuid = payload["uuid"]
        reason = payload["reason"]
        entity = payload["entity"]
        timestamps = payload["timestamp"]
        
        tseen = timestamps["seen"]
        ttimestamp = timestamps["timestamp"]
        tlast = timestamps["lastSeen"]
        tfirst = timestamps["firstSeen"]
        ttype = -1
        
        if "type" in timestamps:
            ttype = timestamps["type"]
        
        tt_seconds = ttimestamp["seconds"]
        tt_nanos = 0
        
        tl_seconds = tlast["seconds"]
        tl_nanos = 0
        
        tf_seconds = tfirst["seconds"]
        tf_nanos = 0
        
        if "nanos" in ttimestamp:
            tt_nanos = ttimestamp["nanos"]
        
        if "nanos" in tlast:
            tl_nanos = tlast["nanos"]
        
        if "nanos" in tfirst:
            tf_nanos = tfirst["nanos"]
        
        ename = entity["name"]
        
        
        eversion= None
        
        if "version" in entity:
            eversion = entity["version"]
            
    
        timestamp_edge = {
            "seconds": tf_seconds,
            "nanos": tf_nanos
        }
    
        if not pfrom in nodes:
            print(f"Error 1 : in edge {key} : From node {pfrom} does not exist")
            continue   

        if not pto in nodes:
            print(f"Error 2 : in edge {key} : From node {pto} does not exist")
            continue
        
        nfrom = nodes[pfrom]
        nto = nodes[pto]
        
        timestamp_from = {
            "seconds": nfrom["tf_seconds"],
            "nanos": nfrom["tf_nanos"]
        }
        
        timestamp_to = {
            "seconds":nto["tl_seconds"],
            "nanos": nto["tl_nanos"]
        }
        
        if datetime.fromtimestamp(tf_seconds).year != 2023:
            from_edge = timestamp_lt(timestamp_from,timestamp_edge)   
            edge_to = timestamp_lt(timestamp_edge,timestamp_to)
            
            if not from_edge:
                print(f"Error 3 : in edge {key} : edge timestamp is inferior to From node {pfrom} timestamp")
        
            if not edge_to:
                print(f"Error 4 : in edge {key} : edge timestamp is superior to To node {pto} timestamp")
        
        if (datetime.fromtimestamp(nfrom["tf_seconds"]).year != 2023) and (datetime.fromtimestamp(nto["tl_seconds"]).year != 2023):
            swap = timestamp_lt(timestamp_from,timestamp_to)
            
            if not swap:
                print(f"Error 5 : in edge {key} : To node {pto} timestamp is superior to From node {pfrom} timestamp")
                

# filepath_nodes = "bare_effects_filtered.jsonl"
# filepath_edges = "bare_edge_effects_filtered.jsonl"


# output_nodes = "nodes.csv"
# output_edges = "edges.csv"

# nodes = []

# with open(filepath_nodes,"r") as f:
    
#     for line in f.readlines():
        
#         payload = json.loads(line)
        
#         key = payload["_key"]
#         created = payload["created"]
#         length = payload["length"]
#         name = payload["name"]
#         timestamps = payload["timestamp"]
#         ptype = payload["type"]
#         uuid = payload["uuid"]
        
#         tseen = timestamps["seen"]
#         ttimestamp = timestamps["timestamp"]
#         tlast = timestamps["lastSeen"]
#         tfirst = timestamps["firstSeen"]
        
#         tt_seconds = ttimestamp["seconds"]
#         tt_nanos = 0
        
#         tl_seconds = tlast["seconds"]
#         tl_nanos = 0
        
#         tf_seconds = tfirst["seconds"]
#         tf_nanos = 0
        
#         if "nanos" in ttimestamp:
#             tt_nanos = ttimestamp["nanos"]
        
#         if "nanos" in tlast:
#             tl_nanos = tlast["nanos"]
        
#         if "nanos" in tfirst:
#             tf_nanos = tfirst["nanos"]
            
#         row = [
#             key,
#             created,
#             length,
#             name,
#             ptype,
#             uuid,
#             tt_seconds,
#             tt_nanos,
#             tf_seconds,
#             tf_nanos,
#             tl_seconds,
#             tl_nanos,
#             tseen
#         ]
        
#         nodes.append(row)


# nodes_header = [
#     "_key",
#     "created",
#     "length",
#     "name",
#     "type",
#     "uuid",
#     "tt_seconds",
#     "tt_nanos",
#     "tf_seconds",
#     "tf_nanos",
#     "tl_seconds",
#     "tl_nanos",
#     "tseen"
# ]

# with open(output_nodes,"w") as f:
    
#     writer = csv.writer(f)
    
#     writer.writerow(nodes_header)
#     for row in nodes:
#         writer.writerow(row)
        
        
# """
# {
#     "_key": "980a76d6-0e55-4009-bf82-801388405023",
#     "_from": "90e36941-ff2f-5ecf-f00e-08b1a1ba9bb7",
#     "_to": "f4310a0e-1c83-57c8-e15f-93abdecbd9a2",
#     "timestamp": {
#         "timestamp": {
#             "seconds": 1699895484,
#             "nanos": 101985210
#         },
#         "firstSeen": {
#             "seconds": 1699895484,
#             "nanos": 101985210
#         },
#         "lastSeen": {
#             "seconds": 1699895484,
#             "nanos": 101985210
#         },
#         "seen": 1,
#         "type": 1
#     },
#     "reason": "CONTAINS",
#     "entity": {
#         "name": "parser",
#         "version": "0.1.0"
#     },
#     "uuid": "980a76d6-0e55-4009-bf82-801388405023"
# }
# """

# edges = []
# with open(filepath_edges,"r") as f:
    
#     for line in f.readlines():
        
#         payload = json.loads(line)
        
#         key = payload["_key"]
#         pfrom = payload["_from"]
#         pto = payload["_to"]
#         uuid = payload["uuid"]
#         reason = payload["reason"]
#         entity = payload["entity"]
#         timestamps = payload["timestamp"]
        
#         tseen = timestamps["seen"]
#         ttimestamp = timestamps["timestamp"]
#         tlast = timestamps["lastSeen"]
#         tfirst = timestamps["firstSeen"]
#         ttype = -1
        
#         if "type" in timestamps:
#             ttype = timestamps["type"]
        
#         tt_seconds = ttimestamp["seconds"]
#         tt_nanos = 0
        
#         tl_seconds = tlast["seconds"]
#         tl_nanos = 0
        
#         tf_seconds = tfirst["seconds"]
#         tf_nanos = 0
        
#         if "nanos" in ttimestamp:
#             tt_nanos = ttimestamp["nanos"]
        
#         if "nanos" in tlast:
#             tl_nanos = tlast["nanos"]
        
#         if "nanos" in tfirst:
#             tf_nanos = tfirst["nanos"]
        
#         ename = entity["name"]
        
        
#         eversion= None
        
#         if "version" in entity:
#             eversion = entity["version"]
            
#         row = [
#             key,
#             pfrom,
#             pto,
#             uuid,
#             reason,
#             tt_seconds,
#             tt_nanos,
#             tf_seconds,
#             tf_nanos,
#             tl_seconds,
#             tl_nanos,
#             tseen,
#             ttype,
#             ename,
#             eversion
#         ]
        
#         edges.append(row)


# edges_header = [
#     "_key",
#     "_from",
#     "_to",
#     "uuid",
#     "reason",
#     "tt_seconds",
#     "tt_nanos",
#     "tf_seconds",
#     "tf_nanos",
#     "tl_seconds",
#     "tl_nanos",
#     "tseen",
#     "ttype",
#     "entity_name",
#     "entity_version"
# ]

# with open(output_edges,"w") as f:
    
#     writer = csv.writer(f)
    
#     writer.writerow(edges_header)
#     for row in edges:
#         writer.writerow(row)
        