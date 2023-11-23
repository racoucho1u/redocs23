#from graph import g
import re



pattern = [["system.identity"], ["START", "STOP"], ["system.process"]]
my_regex = r"\d" + re.escape(pattern) + r"TOTO"

#if re.search(my_regex, subject, re.IGNORECASE):



def get_matched_sequences(pattern, depth):
	matched_sequences = []

	def rec_aux(root, sequence, n):
		if (n == 0):
			flat_sequence = sum([[trio[1],trio[2]] for trio in sequence], [sequence[0][0]])
			if match_sequence_pattern(flat_sequence, pattern):
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


def match_sequence_pattern(flat_seq, pattern):
	return True

#l = get_matched_sequences("pattern", 2)
#for e in l:
#	print(e)

