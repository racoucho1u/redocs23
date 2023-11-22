from graph import g

# 
def activity_hour(hour_start, hour_end, path_file):
    """
    log in path_file every edge whose timestamp is between hour_start and hour_end

    Args:
        hour_start (int)
        hour_end (int)
        path_file (string)
    """

    fd = open(path_file,"w")

    for e in g.edges():
        
        if (e.timestamp()):
        
            if not (hour_start <= e.timestamp().datetime().hour <= hour_end):
                fd.write(f"Warning : activity outside work hours ({hour_start}h - {hour_end}h) detected in edge {e.uuid} at {e.timestamp()}\n")
                

def node_and_emission(reason_edge, type_node, path_file):
    """
    search every couple node -> edge whose type is type_node and reason is reason_edge and log the list in path_file

    Args:
        reason_edge (string)
        type_node (string)
        path_file (string)
    """
    
    fd = open(path_file,"w")
    
    for e in g.edges():
        
        from_type = e.begin().type
        
        if (e.reason == reason_edge) and (type_node in from_type):
            fd.write(f"Warning : edge {e.uuid} with reason {reason_edge} is emitted by {type_node} node {e.begin().uuid} \n")
 
 
 # examples :       
# activity_hour(5,22,"log/sample_night.log")
# node_and_emission("CONNECT","document.","log/sample_file_co.log")