from requests import get
from time import sleep
from os  import environ
from sys import argv

URL   = f"http://{argv[1] if len(argv) > 1 else 'localhost:8080'}"
DELTA = 10 if environ.get('DELTA') is None else int(environ['DELTA'])
TOTAL = 10 if environ.get('TOTAL') is None else int(environ['TOTAL'])

output = open(f"measurment{str(DELTA)}-{str(TOTAL)}.csv","w")

spout_fields  = ["spoutId", "completeLatency", "failed"]
bolt_fields   = ["boltId", "capacity", "executeLatency", "processLatency", "acked","executors", "tasks"]
custom_fields = ["throughput"]
#worker_fields = ["supervisorId", "executorsTotal", "assignedCpu", "assignedMemOnHeap", "assignedMemOffHeap"]

# 1. get Topology Id
topo_id = get(URL+"/api/v1/topology/summary").json()['topologies'][0]['id']    

# 2. header
temp = get(URL+"/api/v1/topology/"+topo_id+"/metrics").json()

output.write(",".join([",".join([ f"spout{str(i)}-{k}" for k in spout_fields ]) for i,j in enumerate(temp['spouts'])])+",")
output.write(",".join([",".join([ f"bolt{str(i)}-{k}" for k in bolt_fields + custom_fields ]) for i,j in enumerate(temp['bolts'])])+"\n")
##output.write([ ",".join([ "worker" + str(i) + "-" + k for k in worker_fields ]) for i,j in enumerate(temp['workers'])][0]+"\n")


# 3. requests
prev = { 'spouts': list(), 'bolts': list() }
for i in range(TOTAL):
    curr = get(URL+"/api/v1/topology/"+topo_id+"/metrics").json()

    spouts = sorted([[ spout[j] for j in spout_fields] for spout in curr['spouts']], key = lambda x: x[0])
    bolts  = sorted([[ bolt[j] for j in bolt_fields] for bolt in curr['bolts']], key = lambda x: x[0])
    #workers = { worker['[ worker[j] for j in worker_fields] for worker in curr['workers']]

    if prev['bolts'] != list():
        acked = bolt_fields.index('acked')
        tp = [(i[acked] - j[acked])/DELTA for i,j in zip(bolts,prev['bolts']) ] 
        for i in range(len(tp)):
            bolts[i].append(tp[i])
    else:
        for i in range(len(bolts)):
            bolts[i].append(0);

    print("-------------------------------------")
    print(spouts)
    print(bolts)
    
    prev['spouts'] = spouts
    prev['bolts']  = bolts

    output.write(",".join([ ",".join([ str(j) for j in i ]) for i in spouts ]) + ",")
    output.write(",".join([ ",".join([ str(j) for j in i ]) for i in bolts ]) + "\n")
    ##output.write(",".join([ ",".join([ str(j) for j in i ]) for i in workers ]) + "\n")

    sleep(DELTA)

output.close()
