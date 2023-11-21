from datetime import datetime
from graph import g
    
    
for e in g.edges():
    
    nfrom = e.begin()
    nto = e.end()
    
    if not nfrom:
        print("Error 1")
        #print(f"Error 1 : in edge {key} : From node {pfrom} does not exist")
        continue   

    if not nto:
        print("Error 2")
        #print(f"Error 2 : in edge {key} : To node {pto} does not exist")
        continue
        
    timestamp_from = nfrom.first_seen()
    timestamp_to = nto.last_seen()
    
    if e.timestamp():
        from_edge = (timestamp_from <= e.timestamp())
        edge_to = (e.timestamp() <= timestamp_to)
        
        if not from_edge:
            print(f"Error 3")
            #print(f"Error 3 : in edge {key} : edge timestamp is inferior to From node {pfrom} timestamp")
    
        if not edge_to:
            print("Error 4")
            #print(f"Error 4 : in edge {key} : edge timestamp is superior to To node {pto} timestamp")
        
    if (timestamp_from) and (timestamp_to):
        swap = (timestamp_from <= timestamp_to)
        
        if not swap:
            print("Error 5")
            #print(f"Error 5 : in edge {key} : To node {pto} timestamp is inferior to From node {pfrom} timestamp")