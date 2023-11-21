from datetime import datetime
import json




nodes_filename = "bare_effects_filtered.jsonl"
edges_filename = "bare_edge_effects_filtered.jsonl"


INFO  = " \033[32m[INFO]\033[0m : "
WARN  = " \033[33m[WARN]\033[0m : "
ERROR = "\033[31m[ERROR]\033[0m : "


# Analyse Timestamp
analyse_datetime = "2023-01-01T00:00:00"
analyse_nanos = int(datetime.fromisoformat(analyse_datetime).timestamp()*1e9)

# String Json Key
SJK_KEY = "_key"
SJK_FROM = "_from"
SJK_TO = "_to"
SJK_TIMESTAMP = "timestamp"
SJK_FIRSTSEEN = "firstSeen"
SJK_LASTSEEN = "lastSeen"
SJK_SECONDS = "seconds"
SJK_NANOS = "nanos"
SJK_TYPE = "type"
SJK_REASON = "reason"
SJK_ENTITY = "entity"
SJK_NAME = "name"

# Alternative Value
AV_NANOS = 0
AV_NAME = "NaN"
AV_TYPE = "unknown"


class Timestamp:

	def __init__(self, timestamp_dict=None):
		if timestamp_dict is None:
			self._nanos = 0
		else:
			self._nanos = timestamp_dict.get(SJK_NANOS, AV_NANOS) + 1e9*timestamp_dict.get(SJK_SECONDS)
			if self._nanos >= analyse_nanos:
				self._nanos = 0

	def __str__(self):
		return datetime.fromtimestamp(round(self._nanos/1e9)).strftime("%d/%m/%Y %H:%M:%S") + ".{:09}".format(self._nanos%1e9)

	def __eq__(self, other):
		return self._nanos == other._nanos

	def __ne__(self, other):
		return self._nanos != other._nanos

	def __lt__(self, other):
		return self._nanos < other._nanos

	def __gt__(self, other):
		return self._nanos > other._nanos

	def __le__(self, other):
		return self._nanos <= other._nanos

	def __ge__(self, other):
		return self._nanos >= other._nanos

	def is_in(guess):
		return guess._timestamp_a <= self._nanos and self._nanos <= guess._timestamp_b

	def compare_to_guess(guess):
		if self.is_in(guess):
			return 0
		elif self._nanos < guess.timestamp_a:
			return -1
		else:
			return 1




class Range:

	def __init__(self, timestamp_a, timestamp_b):
		self._timestamp_a = timestamp_a
		self._timestamp_b = timestamp_b




class Node:

	#def __init__(self, json_line):
	def __init__(self, first_seen=None, last_seen=None, node_type=AV_TYPE):
		#raw_dict = json.loads(json_line)
		#self._key = raw_dict.get(SJK_KEY)
		self._first_seen = first_seen
		#self._first_seen = Timestamp(raw_dict.get(SJK_TIMESTAMP).get(SJK_FIRSTSEEN))
		self._last_seen = last_seen
		#self._last_seen = Timestamp(raw_dict.get(SJK_TIMESTAMP).get(SJK_LASTSEEN))
		self._type = node_type
		#self._type = raw_dict.get(SJK_TYPE)




class Edge:

	#def __init__(self, json_line):
	def __init__(self, node_from, node_to, timestamp, reason, name):
		#raw_dict = json.loads(json_line)
		#self._key = raw_dict.get(SJK_KEY)
		self._from = node_from
		#self._from = raw_dict.get(SJK_FROM)
		self._to = node_to
		#self._to = raw_dict.get(SJK_TO)
		self._timestamp = timestamp
		#self._timestamp = Timestamp(raw_dict.get(SJK_TIMESTAMP).get(SJK_FIRSTSEEN))
		self._reason = reason
		#self._reason = raw_dict.get(SJK_REASON)
		self._name = name	
		#self._name = raw_dict.get(SJK_ENTITY, dict()).get(SJK_NAME, AV_NAME)	


	def ntime(self):
		return self._timestamp._nanos


class Graph:

	def __init__(self, raw_nodes_filename, raw_edges_filename):
		self._raw_nodes_filename = raw_nodes_filename
		self._raw_edges_filename = raw_edges_filename


		self._load_nodes()
		self._load_edges()
		self._buid_graph()



	def _load_nodes(self):
		print("load nodes : xxx.xx%", end="")
		self._nodes_dict = dict()
		with open(self._raw_nodes_filename, "r") as fd:
			lines = fd.readlines()
		self.nb_nodes = len(lines)
		for i, line in enumerate(lines):
			if (i%100==0):
				print("\b\b\b\b\b\b\b{:6.2f}%".format(i/self.nb_nodes*100), end="")
			n = json.loads(line)
			self._nodes_dict[n[SJK_KEY]] = Node(Timestamp(n[SJK_TIMESTAMP][SJK_FIRSTSEEN]), Timestamp(n[SJK_TIMESTAMP][SJK_LASTSEEN]), n[SJK_TYPE])
		print("\b\b\b\b\b\b\bDone   ")

	def _load_edges(self):
		print("load edges : xxx.xx%", end="")
		self._edges_dict = dict()
		with open(self._raw_edges_filename, "r") as fd:
			lines = fd.readlines()
		self.nb_edges = len(lines)
		for i, line in enumerate(lines):
			if (i%100==0):
				print("\b\b\b\b\b\b\b{:6.2f}%".format(i/self.nb_edges*100), end="")
			e = json.loads(line)
			#print(datetime.fromtimestamp(e[SJK_TIMESTAMP][SJK_FIRSTSEEN]["seconds"]).strftime("%d/%m/%Y %H:%M:%S"))
			self._edges_dict[e[SJK_KEY]] = Edge(e[SJK_FROM], e[SJK_TO], Timestamp(e[SJK_TIMESTAMP][SJK_FIRSTSEEN]), e[SJK_REASON], e.get(SJK_ENTITY, dict()).get(SJK_NAME, AV_NAME))
		print("\b\b\b\b\b\b\bDone   ")

	def _buid_graph(self):
		print("build graph : xxx.xx%", end="")

		self._outs = dict()
		self._ins = dict()
		self._ghost_nodes = set()

		nb_missing_node = 0
		i = 0
		for key in self._edges_dict:
			if (i%100==0):
				print("\b\b\b\b\b\b\b{:6.2f}%".format(i/self.nb_edges*100), end="")
			edge = self._edges_dict[key]

			if not (edge._from in self._nodes_dict):
				#print(f"edge.key='{key}' -> {edge._from=} not in nodes !")
				#self._nodes_dict[edge._from] = Node()
				self._ghost_nodes.add(edge._from)
				nb_missing_node += 1
			if not (edge._to in self._nodes_dict):
				#print(f"edge.key='{key}' -> {edge._to=} not in nodes !")
				#self._nodes_dict[edge._to] = Node()
				self._ghost_nodes.add(edge._to)
				nb_missing_node += 1

			if not edge._from in self._outs:
				self._outs[edge._from] = []
			self._outs[edge._from].append(key)

			if not edge._to in self._ins:
				self._ins[edge._to] = []
			self._ins[edge._to].append(key)

			i += 1
		print("\b\b\b\b\b\b\bDone   ")
		self.nb_nodes = len(self._nodes_dict)
		print(f"{nb_missing_node=}")

	def edges(self):
		return list(self._edges_dict.values())

	def nodes(self):
		return list(self._nodes_dict.values())

	def edges_from(self, node_key):
		return [self._edges_dict[edge_key] for edge_key in self._outs[node_key] ]


	def edges_to(self, node_key):
		return [self._edges_dict[edge_key] for edge_key in self._ins[node_key] ]



g = Graph(nodes_filename, edges_filename)



