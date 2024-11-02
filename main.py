from Include import *
from Simulator import *
from TraceGen import *
from multiprocessing import Process

if __name__ == "__main__":
    day = 1
    functionMap, invocationMap = load_data("/Users/jiaruiye/Code/Serverless/dataset", day)
    if len(functionMap) == 0:
        functionMap, invocationMap = parse_data("/Users/jiaruiye/Code/Serverless/dataset", day)
        
    policies=["LRU","LFU","GD","FREQ","SIZE","TTL","RAND"]
    for policy in policies:
        simulator = Simulator(2e3, functionMap, invocationMap, policy, timeLimit=1000, functionLimit=100, logInterval=1e3)
        simulator.run()
        simulator.dumpStats(f"./log")

    l=[]
    for policy in policies:
        # summary
        with open(f"log/{policy}.csv","r") as f:
            lines = f.readlines()
            time,coldStartTime,memorySize,excutingTime,nColdStart,nExcution = lines[-1].split(",")
            l.append([policy, float(coldStartTime), float(memorySize), float(excutingTime), float(nColdStart), float(nExcution)])
    l.sort(key=lambda x:x[1])
    for i in l:
        print(i)