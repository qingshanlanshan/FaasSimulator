from time import sleep
from Include import *
from Simulator import *
from TraceGen import *
from multiprocessing import Process
from threading import Thread

def runSimulation(memorySize, functionMap, invocationMap, policy, timeLimit, functionLimit, logInterval):
    simulator = Simulator(memorySize, functionMap, invocationMap, policy, timeLimit, functionLimit, logInterval, True)
    simulator.run()
    simulator.dumpStats(f"./log")

if __name__ == "__main__":
    day = 1
    dataLocation = "/home/jiarui/Serverless/dataset"
    policies=["LRU","LFU","GD","FREQCOST","FREQSIZE","COSTSIZE","SIZE","COST","TTL","RAND"]
    memoryBudget = 4e3
    # in min
    timeLimit = 100
    functionLimit = 200
    # in ms
    logInterval = 1e3
    
    
    functionMap, invocationMap = load_data(dataLocation, day)
    if len(functionMap) == 0:
        functionMap, invocationMap = parse_data(dataLocation, day)
    procs:list[Process] = []
    for i,policy in enumerate(policies):
        proc = Process(target=runSimulation, args=(memoryBudget, functionMap, invocationMap, policy, timeLimit, functionLimit, logInterval))
        procs.append(proc)
        proc.start()
    for proc in procs:
        proc.join()
    sleep(0.1)
    l=[]
    for policy in policies:
        # summary
        with open(f"log/{policy}.csv","r") as f:
            lines = f.readlines()
            minMemoryReq = float(lines[0].split(",")[1])
            time,coldStartTime,memorySize,excutingTime,nColdStart,nExcution = lines[-1].split(",")
            l.append([policy, float(coldStartTime), float(memorySize), float(excutingTime), float(nColdStart), float(nExcution), minMemoryReq])
    l.sort(key=lambda x:x[1])
    print(" Policy, ColdStartTime, MemorySize, ExcutingTime, NColdStart, NExcution, PeakMemory")
    for i in l:
        print(i)