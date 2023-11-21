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
SJK_SEEN = "seen"
SJK_TYPE = "type"
SJK_REASON = "reason"
SJK_ENTITY = "entity"
SJK_NAME = "name"

# Alternative Value
AV_NANOS = 0
AV_NAME = "NaN"
AV_TYPE = "ghost_node"


class Timestamp:

	def __init__(self, timestamp_dict=None):
		if timestamp_dict is None:
			self._nanos = 0
		else:
			self._nanos = timestamp_dict.get(SJK_NANOS, AV_NANOS) + 1e9*timestamp_dict.get(SJK_SECONDS)
			if self._nanos >= analyse_nanos:
				self._nanos = 0

	def __hash__(self):
		#print(type(hash(self._nanos)))
		return hash(self._nanos)

	def __str__(self):
		return datetime.fromtimestamp(round(self._nanos/1e9)).strftime("%d/%m/%Y %H:%M:%S") + ".{:09}".format(self._nanos%1e9)

	def __bool__(self):
		return not (self._nanos == 0)

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

	def is_in(self, guess):
		return guess._timestamp_a <= self._nanos and self._nanos <= guess._timestamp_b

	def compare_to_guess(self, guess):
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

	def __init__(self, uuid="00000000-0000-0000-0000-000000000000", first_seen=None, last_seen=None, node_type=AV_TYPE, seen = 0):
		self.uuid = uuid
		self._first_seen = first_seen
		self._last_seen = last_seen
		self.type = node_type
		self.seen = seen
		self.estimated_first = None
		self.estimated_last = None

	def __bool__(self):
		return not self.type==AV_TYPE

	def first_seen(self):
		# return a timestamp object
		return self._first_seen

	def last_seen(self):
		# return a timestamp object
		return self._last_seen




class Edge:

	def __init__(self, uuid, uuid_from, uuid_to, timestamp, reason, name, seen):
		self.uuid = uuid
		self._uuid_from = uuid_from
		self._uuid_to = uuid_to
		self._timestamp = timestamp
		self.reason = reason
		self.name = name	
		self.seen = seen
		self.estimated_first = None
		self.estimated_last = None

	def same_nodes(self, other):
		return ((self._uuid_from == other._uuid_from and self._uuid_to == other._uuid_to) or (self._uuid_from == other._uuid_to and self._uuid_to == other._uuid_from))

	def begin(self):
		if self._uuid_from in g._ghost_nodes:
			return Node()
		else:
			return g._nodes_dict[self._uuid_from]

	def end(self):
		if self._uuid_to in g._ghost_nodes:
			return Node()
		else:
			return g._nodes_dict[self._uuid_to]

	def reason(self):
		print("reason() is deprecated, use reason without parenthese")
		return self.reason

	def timestamp(self):
		# return a timestamp object
		return self._timestamp

	def ntime(self):
		print("ntime() is deprecated, use timestamp()")
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
			self._nodes_dict[n[SJK_KEY]] = Node(n[SJK_KEY], Timestamp(n[SJK_TIMESTAMP][SJK_FIRSTSEEN]), Timestamp(n[SJK_TIMESTAMP][SJK_LASTSEEN]), n[SJK_TYPE], n[SJK_TIMESTAMP][SJK_SEEN])
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
			self._edges_dict[e[SJK_KEY]] = Edge(e[SJK_KEY], e[SJK_FROM], e[SJK_TO], Timestamp(e[SJK_TIMESTAMP][SJK_FIRSTSEEN]), e[SJK_REASON], e.get(SJK_ENTITY, dict()).get(SJK_NAME, AV_NAME), e[SJK_TIMESTAMP][SJK_SEEN])
		print("\b\b\b\b\b\b\bDone   ")

	def _buid_graph(self):
		print("build graph : xxx.xx%", end="")

		self._outs = dict()
		self._ins = dict()
		self._ghost_nodes = set()

		i = 0
		for key in self._edges_dict:
			if (i%100==0):
				print("\b\b\b\b\b\b\b{:6.2f}%".format(i/self.nb_edges*100), end="")
			edge = self._edges_dict[key]

			if not (edge._uuid_from in self._nodes_dict):
				#print(f"edge.key='{key}' -> {edge._from=} not in nodes !")
				#self._nodes_dict[edge._from] = Node()
				self._ghost_nodes.add(edge._uuid_from)
			if not (edge._uuid_to in self._nodes_dict):
				#print(f"edge.key='{key}' -> {edge._to=} not in nodes !")
				#self._nodes_dict[edge._to] = Node()
				self._ghost_nodes.add(edge._uuid_to)

			if not edge._uuid_from in self._outs:
				self._outs[edge._uuid_from] = []
			self._outs[edge._uuid_from].append(key)

			if not edge._uuid_to in self._ins:
				self._ins[edge._uuid_to] = []
			self._ins[edge._uuid_to].append(key)

			i += 1
		print("\b\b\b\b\b\b\bDone   ")
		self.nb_nodes = len(self._nodes_dict)
		print(f"nb ghost nodes = {len(self._ghost_nodes)}")

	def edges(self):
		return list(self._edges_dict.values())

	def nodes(self):
		return list(self._nodes_dict.values())

	def edges_from(self, node_uuid):
		return [self._edges_dict[edge_key] for edge_key in self._outs.get(node_uuid, []) ]

	def edges_to(self, node_uuid):
		return [self._edges_dict[edge_key] for edge_key in self._ins.get(node_uuid, []) ]



g = Graph(nodes_filename, edges_filename)

timestamps = set()
for node in g.nodes():
	if t:=node.first_seen():
		timestamps.add(t)
	if t:=node.last_seen():
		timestamps.add(t)
for edge in g.edges():
	if t:=edge.timestamp():
		timestamps.add(t)


LAST_TIMESTAMP = max(timestamps)
FIRST_TIMESTAMP = min(timestamps)



