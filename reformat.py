#import re
import json

nodes_filename = "bare_effects_filtered.jsonl"
reformated_nodes_filename = "reformated_nodes.jsonl"
edges_filename = "bare_edge_effects_filtered.jsonl"
reformated_edges_filename = "reformated_edges.jsonl"



#class Edge:
#
#	def __init__(self, line):
#		rop = json.loads(line)
#		self._
#		self._key = rop.get("_key")
#		self._from = rop.get("_from")
#		self._to = rop.get("_to")
#		self._firstseen = dict()
#		self._firstseen["seconds"] = rop.get("timestamp").get("firstSeen").get("seconds")
#		self._firstseen["nanos"] = rop.get("timestamp").get("firstSeen").get("nanos", 0)
#		self._reason = rop.get("reason")
#		self._name = rop.get("entity", dict()).get("name","NaN")


def reformat_nodes_file(nodes_original_filename, nodes_formated_filename):
	print("reformat nodes file ...")
	fd_in = open(nodes_original_filename, "r")
	fd_out = open(nodes_formated_filename, "w")
	lines = fd_in.readlines()
	n = len(lines)
	for i, line in enumerate(lines):
		if (i%10000==0):
			print("\r{:6.2f}%".format(i/n*100), end="")
		dict_in = json.loads(line)
		dict_out = dict()
		dict_out["_key"] = dict_in.get("_key")
		dict_out["_timestamp"] = dict()
		dict_out["_timestamp"]["_firstSeen"] = dict()
		dict_out["_timestamp"]["_firstSeen"]["seconds"] = dict_in.get("timestamp").get("firstSeen").get("seconds")
		dict_out["_timestamp"]["_firstSeen"]["nanos"] = dict_in.get("timestamp").get("firstSeen").get("nanos",0)
		dict_out["_timestamp"]["_lastSeen"] = dict()
		dict_out["_timestamp"]["_lastSeen"]["seconds"] = dict_in.get("timestamp").get("lastSeen").get("seconds")
		dict_out["_timestamp"]["_lastSeen"]["nanos"] = dict_in.get("timestamp").get("lastSeen").get("nanos",0)
		dict_out["_type"] = dict_in.get("type")
		fd_out.write(f"{json.dumps(dict_out)}\n")
	print("\r100.00%\n")


def reformat_edges_file(edges_original_filename, edges_formated_filename):
	print("reformat edges file ...")
	fd_in = open(edges_original_filename, "r")
	fd_out = open(edges_formated_filename, "w")
	lines = fd_in.readlines()
	n = len(lines)
	for i, line in enumerate(lines):
		if (i%10000==0):
			print("\r{:6.2f}%".format(i/n*100), end="")
		dict_in = json.loads(line)
		dict_out = dict()
		dict_out["_key"] = dict_in.get("_key")
		dict_out["_from"] = dict_in.get("_from")
		dict_out["_to"] = dict_in.get("_to")
		dict_out["_firstSeen"] = dict()
		dict_out["_firstSeen"]["seconds"] = dict_in.get("timestamp").get("firstSeen").get("seconds")
		dict_out["_firstSeen"]["nanos"] = dict_in.get("timestamp").get("firstSeen").get("nanos",0)
		dict_out["_reason"] = dict_in.get("reason")
		dict_out["_name"] = dict_in.get("entity", dict()).get("name","NaN")
		fd_out.write(f"{json.dumps(dict_out)}\n")
	print("\r100.00%\n")



reformat_nodes_file(nodes_filename, reformated_nodes_filename)
reformat_edges_file(edges_filename, reformated_edges_filename)