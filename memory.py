from time import sleep
from Include import *
from Simulator import *
from TraceGen import *
from multiprocessing import Process
from threading import Thread

def runSimulation(memorySize, functionMap, invocationMap, policy, timeLimit, functionLimit, logInterval):
    stats = []
    for memorySize in tqdm(np.linspace(1e3, 7e3, 700, dtype=int), desc=f"Policy: {policy}", position=current_process()._identity[0]):
        simulator = Simulator(memorySize, functionMap, invocationMap, policy, timeLimit, functionLimit, logInterval)
        simulator.run()
        stats.append((memorySize,simulator.stats[-1]))
    with open(f"log/{policy}.csv","w") as f:
        f.write("Time, ColdStartTime, MemorySize, ExcutingTime, NColdStart, NExcution, MemoryBudget\n")
        for memorySize,stat in stats:
            f.write(f"{stat.time}, {stat.coldStartTime}, {stat.memorySize}, {stat.excutingTime}, {stat.nColdStart}, {stat.nExcution}, {memorySize}\n")

if __name__ == "__main__":
    day = 1
    dataLocation = "/home/jiarui/Serverless/dataset"
    policies=["LRU","LFU","GD","FREQCOST","FREQSIZE","COSTSIZE","TTL","RAND"]
    memoryBudget = 5e3
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