import subprocess
"""Finds all runnig processes that match process_name.
Input:    name of process to look for across cluster
Returns:  dict w/ keys=nodes, vals=list w/ps output of each process found

"""


def determine_processes(process_name):
    my_ret = {}
    cmd_args = ['isi_for_array', '-X', 
                'ps', 'auxwww', 
                '|', 
                'grep', process_name, 
                '|', 'grep', '-v', 'grep']
    run_ps = subprocess.Popen(cmd_args, stdout=subprocess.PIPE)
    grepped_output = run_ps.communicate()

    list_of_grepped_output = grepped_output[0].split('\n')

    for i in list_of_grepped_output:
        if i and not i.endswith('exited with status 1'):
            node = i.split(' ')[0].strip(':')        
            if not node in my_ret.keys():
                my_ret[node] = []
            my_ret[node].append(i)
           

    return my_ret
