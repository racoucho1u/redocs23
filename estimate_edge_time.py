import json
import csv
from datetime import datetime

filepath_nodes = "../REDOCS23/bare_effects_filtered.jsonl"
filepath_edges = "../REDOCS23/bare_edge_effects_filtered.jsonl"

estimated_edges = []
estimated_edges_file = "estimated_edges.json"

output_nodes = "nodes.csv"
output_edges = "edges.csv"

nodes = {}
edges = {}


def noTimeNode(node):
	return()


def noTimeEdge(tf_seconds,tf_nano,nfrom,nto):

	changed = False
	dic = {
		"changed": changed,
		"estimated_before_seconds": tf_seconds,
		"estimated_before_nanos": tf_nanos,
		"estimated_after_seconds": tf_seconds,
		"estimated_after_nanos": tf_nanos
		}

	if datetime.fromtimestamp(tf_seconds).year == 2023:

		tf_from = datetime.fromtimestamp(nfrom["tf_seconds"]).year
		tf_to = datetime.fromtimestamp(nto["tl_seconds"]).year

		estimated_before_sec = 0
		estimated_before_nano = 0
		estimated_after_sec = nto["tl_seconds"]
		estimated_after_nano = nto["tl_nanos"]

		if (tf_from != 2023):
			estimated_before_sec = nfrom["tf_seconds"]
			estimated_before_nano = nfrom["tf_nanos"]
			changed = True
		else:
			noTimeNode(nfrom)
		if (tf_to != 2023):
			#estimated_after = nto["tl_seconds"]
			changed = True
		else:
			noTimeNode(nto)

		dic = {
			"changed": changed,
			"estimated_before_seconds": estimated_before_sec,
			"estimated_before_nanos": estimated_before_nano,
			"estimated_after_seconds": estimated_after_sec,
			"estimated_after_nanos": estimated_after_nano
			}
		
	return(dic)

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
        test = False
        testfirst = tfirst
        testlast =  tlast
        
        tt_seconds = ttimestamp["seconds"]
        tt_nanos = 0
        
        tl_seconds = tlast["seconds"]
        tl_nanos = 0
        
        tf_seconds = tfirst["seconds"]
        tf_nanos = 0

        tle_seconds = tlast["seconds"]
        tle_nanos = 0
        
        tfe_seconds = 0
        tfe_nanos = 0        
        
        if "nanos" in ttimestamp:
            tt_nanos = ttimestamp["nanos"]
        
        if "nanos" in tlast:
            tl_nanos = tlast["nanos"]

        if "nanos" in tlast:
            tle_nanos = tlast["nanos"]
        
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
            "tfe_seconds" : tf_seconds,
            "tfe_nanos" : tf_nanos,
            "tle_seconds" : tl_seconds,
            "tle_nanos" : tl_nanos,
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
        
        dic = noTimeEdge(tf_seconds,tf_nanos,nfrom,nto)

        row = {
            "_key":key,
            "_from":pfrom,
            "_to":pto,
            "timestamp": {
			 	"timestamp": {"seconds": tt_seconds, "nanos": tt_nanos}, 
			 	"firstSeen": {"seconds": tf_seconds, "nanos": tf_nanos}, 
			 	"lastSeen": {"seconds": tl_seconds, "nanos": tl_nanos},
			 	# A changer par les resultat du dict + prendre en compte les nanos dans la fonction
			 	"estimation": dic["changed"],
			 	"estimatedFst": {"seconds": dic["estimated_before_seconds"], "nanos": dic["estimated_before_nanos"]},
			 	"estimatedLast": {"seconds": dic["estimated_after_seconds"], "nanos": dic["estimated_after_nanos"]},
			 	"seen": tseen}, 
            "reason":reason,
            "entity":{"name":ename,"version":eversion},
            "uuid":uuid
        }
        
        estimated_edges.append(row)

with open("estimated_edges_file",'a') as eef:
	for e in estimated_edges:
		json.dump(e,eef)