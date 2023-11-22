from datetime import datetime, timezone, timedelta
import json
import os
import pickle
import hashlib



nodes_filename = "bare_effects_filtered.jsonl"
edges_filename = "bare_edge_effects_filtered.jsonl"



INFO  = " \033[32m[INFO]\033[0m : "
WARN  = " \033[33m[WARN]\033[0m : "
ERROR = "\033[31m[ERROR]\033[0m : "

# Timezone for chronological analysis
# hour=offset
TIMEZONE = timezone(timedelta(hours=0)) # (UTC−00:00)

# Analysis timestamp
analyse_datetime = "2023-01-01T00:00:00+00:00" # timezone analyse can be different
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

# globals
FIRST_TIMESTAMP = None
LAST_TIMESTAMP = None




class Timestamp:

	def __init__(self, timestamp_dict=None):
		if timestamp_dict is None:
			self._nanos = -1
		else:
			self._nanos = timestamp_dict.get(SJK_NANOS, AV_NANOS) + 1e9*timestamp_dict.get(SJK_SECONDS)
			if self._nanos >= analyse_nanos:
				self._nanos = -1

	def datetime(self):
		return datetime.fromtimestamp(round(self._nanos/1e9), tz=TIMEZONE)


	def __hash__(self):
		return hash(self._nanos)

	def __str__(self):
		if self:
			return datetime.fromtimestamp(round(self._nanos/1e9), tz=TIMEZONE).strftime("%d/%m/%Y %H:%M:%S") + ".{:09}".format(self._nanos%1e9)
		else:
			return "uninitialized timestamp"

	def __bool__(self):
		return not (self._nanos == -1)

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


	def estimated_cmp(self, node_or_edge):
		if self < node_or_edge.estimated_first:
			return -1
		elif self > node_or_edge.estimated_last:
			return 1
		else:
			return 0



class Node:

	def __init__(self, uuid, first_seen=None, last_seen=None, node_type=AV_TYPE, seen = 0):
		self.uuid = uuid
		self._first_seen = first_seen
		self._last_seen = last_seen
		self.type = node_type
		self.seen = seen

	def _build(self):
		if self._first_seen:
			self.estimated_first = self._first_seen
		else:
			self.estimated_first = FIRST_TIMESTAMP
		if self._last_seen:
			self.estimated_last = self._last_seen
		else:
			self.estimated_last = LAST_TIMESTAMP

	def __bool__(self):
		return not self.type==AV_TYPE

	def first_seen(self):
		# return a timestamp object
		return self._first_seen

	def last_seen(self):
		# return a timestamp object
		return self._last_seen

	def estimated_cmp(self, timestamp):
		return timestamp.estimated_cmp(self)

	def mu(self):
		return self.estimated_last._nanos - self.estimated_first._nanos




class Edge:

	def __init__(self, uuid, uuid_from, uuid_to, timestamp, reason, name, seen):
		self.uuid = uuid
		self._uuid_from = uuid_from
		self._uuid_to = uuid_to
		self._timestamp = timestamp
		self.reason = reason
		self.name = name	
		self.seen = seen

	def _build(self):
		if self._timestamp:
			self.estimated_first = self._timestamp
			self.estimated_last = self._timestamp
		else:
			self.estimated_first = FIRST_TIMESTAMP
			self.estimated_last = LAST_TIMESTAMP


	def check_end_timestamps(self):
		return self.begin().estimated_cmp(self._timestamp)

	def check_begin_timestamps(self):
		return self.end().estimated_cmp(self._timestamp)

	def same_nodes(self, other):
		return ((self._uuid_from == other._uuid_from and self._uuid_to == other._uuid_to) or (self._uuid_from == other._uuid_to and self._uuid_to == other._uuid_from))

	def begin(self):
		return g.nodes_dict[self._uuid_from]

	def end(self):
		return g.nodes_dict[self._uuid_to]

	def timestamp(self):
		# return a timestamp object
		return self._timestamp

	def estimated_cmp(self, timestamp):
		return timestamp.estimated_cmp(self)

	def mu(self):
		return self.estimated_last._nanos - self.estimated_first._nanos




class Graph:
	"""
	objet graph
	peut charger un graph depuis les fichier json (raw data), ou depuis un fichier data si il existe un graph déja sauvegardé
	"""

	def __init__(self, raw_nodes_filename, raw_edges_filename, backup_mode=True):
		self._raw_nodes_filename = raw_nodes_filename
		self._raw_edges_filename = raw_edges_filename
		self._backup_mode = backup_mode
		self._backup_filename = "." + hashlib.sha1((self._raw_nodes_filename + self._raw_edges_filename + "0004").encode()).hexdigest().zfill(40)[:16] + ".data"

		if self._backup_mode:
			print("backup mode enable")
			print("remember to save the graph before closing : using g.save()")
			if os.path.exists(self._backup_filename):
				self.load()
				return

		self._load_nodes()
		self._load_edges()
		self._init_globals()
		self._build()
		for e in self.edges():
			e._build()
		for n in self.nodes():
			n._build()
		if self._backup_mode:
			self.save()
		#print("mu={:25.0f}".format((LAST_TIMESTAMP._nanos - FIRST_TIMESTAMP._nanos)*(len(self.edges())+len(self.nodes()))))
		#print("mu={:25.0f}".format(self.mu()))


	def load(self):
		"""
		méthode de chargement depuis un fichier de sauvegarde
		"""

		if not self._backup_mode:
			print("backup mode disable")
			return

		print("graph loading ...")
		f = open(self._backup_filename, 'rb')
		tmp_dict = pickle.load(f)
		f.close()          
		self.__dict__.update(tmp_dict)
		print("graph loaded !")


	def save(self):
		"""
		methode de sauvegarde dans un fichier
		"""

		if not self._backup_mode:
			print("backup mode disable")
			return

		print("graph saving ...")
		f = open(self._backup_filename, 'wb')
		pickle.dump(self.__dict__, f, 2)
		f.close()
		print("graph saved !")


	def _load_nodes(self):
		"""
		chargement du json de node
		initialisation du dictionnaire de node
		"""
		print("load nodes : xxx.xx%", end="")
		self.nodes_dict = dict()
		with open(self._raw_nodes_filename, "r") as fd:
			lines = fd.readlines()
		self.nb_nodes = len(lines)
		for i, line in enumerate(lines):
			if (i%1000==0):
				print("\b\b\b\b\b\b\b{:6.2f}%".format(i/self.nb_nodes*100), end="", flush=True)
			n = json.loads(line)
			self.nodes_dict[n[SJK_KEY]] = Node(n[SJK_KEY], Timestamp(n[SJK_TIMESTAMP][SJK_FIRSTSEEN]), Timestamp(n[SJK_TIMESTAMP][SJK_LASTSEEN]), n[SJK_TYPE], n[SJK_TIMESTAMP][SJK_SEEN])
		print("\b\b\b\b\b\b\bDone   ")

	def _load_edges(self):
		"""
		chargement du json de edge
		initialisation du dictionnaire de edge
		"""
		print("load edges : xxx.xx%", end="")
		self.edges_dict = dict()
		with open(self._raw_edges_filename, "r") as fd:
			lines = fd.readlines()
		self.nb_edges = len(lines)
		for i, line in enumerate(lines):
			if (i%1000==0):
				print("\b\b\b\b\b\b\b{:6.2f}%".format(i/self.nb_edges*100), end="", flush=True)
			e = json.loads(line)
			#print(datetime.fromtimestamp(e[SJK_TIMESTAMP][SJK_FIRSTSEEN]["seconds"]).strftime("%d/%m/%Y %H:%M:%S"))
			self.edges_dict[e[SJK_KEY]] = Edge(e[SJK_KEY], e[SJK_FROM], e[SJK_TO], Timestamp(e[SJK_TIMESTAMP][SJK_FIRSTSEEN]), e[SJK_REASON], e.get(SJK_ENTITY, dict()).get(SJK_NAME, AV_NAME), e[SJK_TIMESTAMP][SJK_SEEN])
		print("\b\b\b\b\b\b\bDone   ")

	def _init_globals(self):
		"""
		initialisation des constant globales
		"""

		global LAST_TIMESTAMP  # dernier timestamp du dataset (hors analyse)
		global FIRST_TIMESTAMP # premier timestamp du dataset

		timestamps = set()
		for node in self.nodes():
			if t:=node.first_seen():
				timestamps.add(t)
			if t:=node.last_seen():
				timestamps.add(t)
		for edge in self.edges():
			if t:=edge.timestamp():
				timestamps.add(t)

		LAST_TIMESTAMP = max(timestamps)
		FIRST_TIMESTAMP = min(timestamps)

	def _build(self):
		print("build graph : xxx.xx%", end="")

		self._outs = dict()
		self._ins = dict()
		self._ghost_nodes = set()

		i = 0
		for key in self.edges_dict:
			if (i%1000==0):
				print("\b\b\b\b\b\b\b{:6.2f}%".format(i/self.nb_edges*100), end="", flush=True)
			edge = self.edges_dict[key]

			if not (edge._uuid_from in self.nodes_dict):
				#print(f"edge.key='{key}' -> {edge._from=} not in nodes !")
				#self.nodes_dict[edge._from] = Node()
				self._ghost_nodes.add(edge._uuid_from)
				self.nodes_dict[edge._uuid_from] = Node(edge._uuid_from)
			if not (edge._uuid_to in self.nodes_dict):
				#print(f"edge.key='{key}' -> {edge._to=} not in nodes !")
				#self.nodes_dict[edge._to] = Node()
				self._ghost_nodes.add(edge._uuid_to)
				self.nodes_dict[edge._uuid_to] = Node(edge._uuid_to)

			if not edge._uuid_from in self._outs:
				self._outs[edge._uuid_from] = []
			self._outs[edge._uuid_from].append(key)

			if not edge._uuid_to in self._ins:
				self._ins[edge._uuid_to] = []
			self._ins[edge._uuid_to].append(key)

			i += 1
		print("\b\b\b\b\b\b\bDone   ")
		self.nb_nodes = len(self.nodes_dict)
		print(f"nb ghost nodes = {len(self._ghost_nodes)}")

	def edges(self):
		return list(self.edges_dict.values())

	def nodes(self):
		return list(self.nodes_dict.values())

	def edges_from(self, node_uuid):
		return [self.edges_dict[edge_key] for edge_key in self._outs.get(node_uuid, []) ]

	def edges_to(self, node_uuid):
		return [self.edges_dict[edge_key] for edge_key in self._ins.get(node_uuid, []) ]

	def mu(self):
		# ammélioration significative du calcul de mu en l'actualisant en temps réel avec __setattr__ sur les estimated
		# construire MU en global initiée par self._init_globals()
		# finalement, peut etre pas necessaire au vu de la fréquence d'appel
		rop = 0
		for e in self.edges():
			rop += e.mu()
		for n in self.nodes():
			rop += n.mu()
		return rop




g = Graph(nodes_filename, edges_filename)





