from graph import g, Node, Edge
import re

ARROW_CHAR = ">"


"""
definition d'un pattern:
liste de lists :
les elements impairs sont des listes de node.type
les elements pairs sont des listes de edge.reason

le pattern est donc une liste de taille impaire
exemple, [liste_de_node.type, liste_de_edge.reason, liste_de_node.type, liste_de_edge.reason, ... , liste_de_node.type]
get_matched_sequences leve une erreur sinon.

note, une liste est une whitecard
exemple de patern:

p = [["system.process"], ["EXECUTE"], ["script.powershell"]]
match:
	system.process -> EXECUTE -> script.powershell

p = [["system.identity"], ["START", "STOP"], ["system.process"]]<
match:
	system.identity -> START -> system.process
	system.identity -> STOP -> system.process

p = [["system.identity"], ["START", "STOP"], []]
match:
	system.identity -> START -> **ANYTHING**
	system.identity -> STOP -> **ANYTHING**

p = [["system.process"], [], ["script.powershell"]]
match:
	system.process -> **ANYTHING** -> script.powershell


note:
get_matched_sequences ne permet pas des matchs conditionnel comme:
	si commence par :       system.identity -> START
	alors match :           system.identity -> START -> typeA
	sinon si commence par : system.identity -> STOP
	alors match :           system.identity -> START -> typeB

	pour cela, il faut merge les resultat de deux appel:
	appel 1 avec p = [["system.identity"], ["START"], [typeA]]
	appel 2 avec p = [["system.identity"], ["STOP"], [typeB]]


"""


def pattern_compilation(l_pattern):
	p = ARROW_CHAR.join(["(?:"+"|".join([re.escape(t) for t in e]) + ")" for e in l_pattern])
	r = p.replace("(?:)", "[^>]*")
	return r


def get_matched_sequences(pattern):
	assert len(pattern)%2 == 1
	depth = int((len(pattern)-1)/2)
	re_pattern = pattern_compilation(pattern)
	matched_sequences = []

	def rec_aux(root, sequence, n):
		if (n == 0):
			flat_sequence = sum([[trio[1],trio[2]] for trio in sequence], [sequence[0][0]])
			if match_sequence_pattern(flat_sequence, re_pattern):
				nonlocal matched_sequences
				matched_sequences.append(flat_sequence)
		else:
			next_edges = g.edges_from(root.uuid)
			for next_edge in next_edges:
				next_node = next_edge.end()
				rec_aux(next_node, sequence + [(root, next_edge, next_node)], n-1)

	for node in g.nodes():
		rec_aux(node, [], depth)
	return matched_sequences


def match_sequence_pattern(flat_seq, re_pattern):
	string_seq = ""
	for elt in flat_seq:
		if   type(elt) == Node:
			string_seq += elt.type
		elif type(elt) == Edge:
			string_seq += elt.reason
		else:
			raise
		string_seq += ARROW_CHAR

	return bool(re.match(re_pattern, string_seq))



l = get_matched_sequences(p)
