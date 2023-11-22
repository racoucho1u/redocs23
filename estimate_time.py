from graph import g
import csv


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

			mini = min(allFst)

			#Track wether the node will be changed
			if (mini != g.nodes_dict[nuuid].estimated_first):
				changed = True
			
			#Getting the first possible call to the node (min first_seen des edges qui arrivent)
			g.nodes_dict[nuuid].estimated_first = mini

		else:
			floating_nodes.append(nuuid)
		
	#Same as above for the after
	if (not g.nodes_dict[nuuid].last_seen()):
		
		allLast = [e.estimated_last for e in eto+efrom]
		if (len(allLast)>0):

			maxi = max(allLast)

			#Track wether the node will be changed
			if (maxi != g.nodes_dict[nuuid].estimated_last):
				changed = True

			#Getting the flast possible call from the node (max last_seen des edges qui arrivent)
			g.nodes_dict[nuuid].estimated_last = maxi

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
		from_uuid = nfrom.uuid
		to_uuid = nto.uuid

		#If the node before has a usable seen_first time
		if (from_time_fst > g.edges_dict[euuid].estimated_first):
			g.edges_dict[euuid].estimated_first = from_time_fst
			changed = True
		#Same thing as above but for the node after
		if (to_time_last < g.edges_dict[euuid].estimated_last):
			g.edges_dict[euuid].estimated_last = to_time_last
			changed = True
			

	#Writing in the log file
	if (changed):
		edges_estimated.append([euuid,g.edges_dict[euuid].estimated_first,g.edges_dict[euuid].estimated_last])

	return(edges_estimated)

def lauchAnalysis(mu,gatherFnodes):

	edges_estimated = []
	nodes_estimated = []
	floating_nodes = []

	for euuid in g.edges_dict:
		edges_estimated = guessTimeEdge(euuid,edges_estimated)
	for nuuid in g.nodes_dict:
		(nodes_estimated,floating_nodes) = guessTimeNode(nuuid,nodes_estimated,floating_nodes)

	#with open("nodes_estimated.log","a") as ne:
	#	for n in nodes_estimated:
	#		ne.write(f"Node: {n[0]} | estimated_first: {n[1]} | estimated_last: {n[2]}")
	#print(f"Changed nodes: {len(nodes_estimated)}")

	#with open("edges_estimated.log","a") as ee:
	#	for e in edges_estimated:
	#		ee.write(f"Edge: {e[0]} | estimated_first: {e[1]} | estimated_last: {e[2]}")
	#print(f"Changed edges: {len(edges_estimated)}")

	with open("nodes_estimated.csv","a") as ne:
		csv_w = csv.writer(ne, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		csv_w.writerow([f"node.uuid", f"node.estimated_first", f"node.estimated_last"])
		for n in nodes_estimated:
			csv_w.writerow([f"{n[0]}", f"{n[1]}", f"{n[2]}"])

	with open("edges_estimated.csv","a") as ee:
		csv_w = csv.writer(ee, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		csv_w.writerow([f"edge.uuid", f"edge.estimated_first", f"edge.estimated_last"])
		for e in edges_estimated:
			csv_w.writerow([f"{e[0]}", f"{e[1]}", f"{e[2]}"])

	if (gatherFnodes):
		with open("floating_nodes.log","a") as fn:
			for n in floating_nodes:
				fn.write(n+"\n")

	print(len(changed_nodes))

	mu.append(g.mu())
	print(mu)
	delta = mu[-2]-mu[-1]	
	print(f"New delta: {delta}")
	return(delta)

mu = [g.mu()]
delta = mu[0]
gatherFnodes = True
changed_nodes = []
while (delta > 0):
	delta = lauchAnalysis(mu,gatherFnodes)
	gatherFnodes = False


g.save()


#while (delta >= pow(10))