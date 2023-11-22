from graph import g


def guessTimeNode(nuuid,nodes_estimated,floating_nodes):

	# Log if a change has been made
	changed = False

	#Get all the edges that lead to this node
	eto = g.edges_to(nuuid)
	efrom = g.edges_from(nuuid)

	#If the first_seen is not usable
	if (not g.nodes_dict[nuuid].first_seen()):

		allFst = [e.estimated_first for e in eto+efrom]
		if (len(allFst)>0):
			#Getting the first possible call to the node (min first_seen des edges qui arrivent)
			g.nodes_dict[nuuid].estimated_first = min(allFst)
			changed = True
		else:
			floating_nodes.append(nuuid)
		
	#Same as above for the after
	if (not g.nodes_dict[nuuid].last_seen()):
		
		allLast = [e.estimated_last for e in eto+efrom]
		if (len(allLast)>0):
			#Getting the first possible call to the node (min first_seen des edges qui arrivent)
			g.nodes_dict[nuuid].estimated_last = max(allLast)
			changed = True
		else:
			floating_nodes.append(nuuid)

	if (changed):
		nodes_estimated.append([nuuid,g.nodes_dict[nuuid].estimated_first,g.nodes_dict[nuuid].estimated_last])

	return((nodes_estimated,floating_nodes))

def guessTimeEdge(euuid,edges_estimated):

	# Log if a change has been made
	changed = False

	#Getting node from, node to and timestamp of the edge
	nfrom = g.edges_dict[euuid].begin()
	nto = g.edges_dict[euuid].end()
	etime = g.edges_dict[euuid].timestamp()

	#If we need to make a guess because the time of the edge cannot be used
	if (not etime):

		from_time_fst = nfrom.estimated_first
		to_time_last = nto.estimated_last

		#If the node before has a usable seen_first time
		if (from_time_fst):
			g.edges_dict[euuid].estimated_first = from_time_fst
			changed = True
		#Same thing as above but for the node after
		if (to_time_last):
			g.edges_dict[euuid].estimated_last = to_time_last
			changed = True

	#Writing in the log file
	if (changed):
		edges_estimated.append([euuid,g.edges_dict[euuid].estimated_first,g.edges_dict[euuid].estimated_last])

	return(edges_estimated)

def lauchAnalysis(mu):

	edges_estimated = []
	nodes_estimated = []
	floating_nodes = []

	for euuid in g.edges_dict:
		edges_estimated = guessTimeEdge(euuid,edges_estimated)
	for nuuid in g.nodes_dict:
		(nodes_estimated,floating_nodes) = guessTimeNode(nuuid,nodes_estimated,floating_nodes)

	with open("nodes_estimated.log","a") as ne:
		for n in nodes_estimated:
			ne.write(f"Node: {n[0]} | estimated_first: {n[1]} | estimated_last: {n[2]}")
	print(len(nodes_estimated))

	with open("edges_estimated.log","a") as ee:
		for e in edges_estimated:
			ee.write(f"Node: {e[0]} | estimated_first: {e[1]} | estimated_last: {e[2]}")
	print(len(edges_estimated))

	with open("floating_nodes.log","a") as fn:
		for n in floating_nodes:
			fn.write(n+"\n")

	mu.append(g.mu())
	print(mu)
	delta = mu[-2]-mu[-1]	
	print(f"New delta: {delta}")
	return(delta)

mu = [g.mu()]
delta = mu
for i in range(3):
	delta = lauchAnalysis(mu)


g.save()


#while (delta >= pow(10))