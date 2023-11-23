from graph import g

def integrity(path_template):
    """_summary_
    
    Args:
        path_template : prefix name of the future log
    
    produce 7 log files :
    Error 1 : the parent node of the edge doesn't exist in the jsonl
    Error 2 : the child node of the edge doesn't exist in the jsonl
    Error 3 : the parent node et the child node of the edge are swap
    Error 4 : the edge is created before the parent node existence
    Error 5 : the edge is created after the parent node existence
    Error 6 : the edge is created before the child node existence
    Error 7 : the edge is created after the child node existence
    """

    fd1 = open(path_template +"001.log","w")
    fd2 = open(path_template +"002.log","w")
    fd3 = open(path_template +"003.log","w")
    fd4 = open(path_template +"004.log","w")
    fd5 = open(path_template +"005.log","w")
    fd6 = open(path_template +"006.log","w")
    fd7 = open(path_template +"007.log","w")
        
        
    for e in g.edges():
        from_edge = 0
        edge_to = 0
        
        nfrom = e.begin()
        nto = e.end()
        
        if not nfrom:
            fd1.write(f"Error 1 : in edge {e.uuid} : From node {nfrom.uuid} does not exist\n")
            continue   

        if not nto:
            fd2.write(f"Error 2 : in edge {e.uuid} : To node {nto.uuid} does not exist\n")
            continue 
        
        if (e.timestamp()): 
            from_edge = e.check_begin_timestamps()
            edge_to = e.check_end_timestamps()
        
        swap = (nto.estimated_last < nfrom.estimated_first)
        
        if swap:
            fd3.write(f"Error 3 : in edge {e.uuid} : To node {nto.uuid} timestamp is inferior to From node {nfrom.uuid} timestamp\n") 
            
        if from_edge == -1:
            fd4.write(f"Error 4 : in edge {e.uuid} : edge is created before From node {nfrom.uuid} existence\n")
            
        if from_edge == 1:
            fd5.write(f"Error 5 : in edge {e.uuid} : edge is created after From node {nfrom.uuid} existence\n")

        if edge_to == -1:
            fd6.write(f"Error 6 : in edge {e.uuid} : edge is created before To node {nto.uuid} existence\n")
            
        if edge_to == 1:
            fd7.write(f"Error 7 : in edge {e.uuid} : edge is created after To node {nto.uuid} existence\n")
                
    fd1.close()
    fd2.close()
    fd3.close()
    fd4.close()
    fd5.close()
    fd6.close()
    fd7.close()
    
integrity("log/estimate_err")