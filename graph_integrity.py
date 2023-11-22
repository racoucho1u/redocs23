from datetime import datetime
from graph import g


fd1 = open("log/err001.log","w")
fd2 = open("log/err002.log","w")
fd3 = open("log/err003.log","w")
fd4 = open("log/err004.log","w")
fd5 = open("log/err005.log","w")
    
    
for e in g.edges():
    from_edge = True
    edge_to = True
    swap = True
    
    nfrom = e.begin()
    nto = e.end()
    
    if not nfrom:
        fd1.write(f"Error 1 : in edge {e.uuid} : From node {nfrom.uuid} does not exist\n")
        continue   

    if not nto:
        fd2.write(f"Error 2 : in edge {e.uuid} : To node {nto.uuid} does not exist\n")
        continue
        
    timestamp_from = nfrom.first_seen()
    timestamp_to = nto.last_seen()
    
    if (e.timestamp()): 
        
        if (timestamp_from):
            from_edge = (timestamp_from <= e.timestamp())
            
        if (timestamp_to):
            edge_to = (e.timestamp() <= timestamp_to)
        
    if (timestamp_from) and (timestamp_to):
        swap = (timestamp_from <= timestamp_to)
        
    if not from_edge:
        fd3.write(f"Error 3 : in edge {e.uuid} : edge timestamp is inferior to From node {nfrom.uuid} timestamp\n")

    if not edge_to:
        fd4.write(f"Error 4 : in edge {e.uuid} : edge timestamp is superior to To node {nto.uuid} timestamp\n")
    
    if not swap:
        fd5.write(f"Error 5 : in edge {e.uuid} : To node {nto.uuid} timestamp is inferior to From node {nfrom.uuid} timestamp\n")  
            
fd1.close()
fd2.close()
fd3.close()
fd4.close()
fd5.close()