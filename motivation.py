from time import sleep
from Include import *
from Simulator import *
from TraceGen import *
from multiprocessing import Pool,Manager
from threading import Thread
from dataclasses import dataclass
from tqdm import tqdm

@dataclass
class Settings:
    memoryBudget: float
    timeLimit: int
    functionLimit: int
    logInterval: int
    
@dataclass
class FuncStats:
    coldStartTime: int
    excutingTime: int
    nColdStart: int
    nExcution: int
    
class MotivSimulator(Simulator):
    def __init__(self, memorySize, functionMap, invocationMap, policy, timeLimit, functionLimit, logInterval, verbose):
        super().__init__(memorySize, functionMap, invocationMap, policy, timeLimit, functionLimit, logInterval, verbose)
        self.funcStats = {}
        
    def process_event(self):
        time, functionId = self.eventQueue[self.step]
        functionInfo = self.functionMap[functionId]
        coldStartTime = 0
        excutingTime = functionInfo.duration
        # try to find a available container with same functionId
        priority = self.findAvailContainer(time, functionId)
        if priority is None:
            # free enough memory for the new container
            self.freeMemory(functionInfo.memory, time)
            # create a new container
            priority = self.newContainer(time, functionId)
            # cold start
            coldStartTime = functionInfo.coldStartTime
            excutingTime += coldStartTime
            self.nColdStart += 1
        self.nExcution += 1
        # sync priority of all containers with the same functionId
        for container in self.cache:
            if container.functionId == functionId:
                container.priority = priority
        
        if functionId not in self.funcStats:
            self.funcStats[functionId] = FuncStats(0,0,0,0)
        self.funcStats[functionId].coldStartTime += coldStartTime
        self.funcStats[functionId].excutingTime += excutingTime
        self.funcStats[functionId].nColdStart += 1 if coldStartTime > 0 else 0
        self.funcStats[functionId].nExcution += 1
        
        # stats
        self.coldStartTime += coldStartTime
        self.excutingTime += excutingTime
        if time - self.lastLogTime > self.logInterval:
            self.lastLogTime = time
            self.stats.append(
                Stats(
                    time,
                    self.coldStartTime,
                    self.memoryUsed,
                    self.excutingTime,
                    self.nColdStart,
                    self.nExcution,
                )
            )
            self.log(
                f"time {time}, coldStartTime {self.coldStartTime}, memoryUsed {self.memoryUsed}, excutingTime {self.excutingTime}, nColdStart {self.nColdStart}, nExcution {self.nExcution}\n",
                filename=f"./log/{self.filename}.log",
            )

if __name__=="__main__":
    settings = {
        "Representative": Settings(
            timeLimit=100,
            functionLimit=400,
            memoryBudget=3e4,
            logInterval=1e3,
        ),
    }
    policy = "GD"
    dataset = "Representative"
    setting = settings[dataset]
    functionMap, invocationMap = load_data("/home/jiarui/Serverless/dataset", 1, dataset)
    simulator = MotivSimulator(
        setting.memoryBudget,
        functionMap,
        invocationMap,
        policy,
        setting.timeLimit,
        setting.functionLimit,
        setting.logInterval,
        True,
    )
    simulator.run()
    simulator.dumpStats(f"./log")
    with open("log", 'w') as f:
        for functionId in simulator.funcStats:
            funcStat = simulator.funcStats[functionId]
            f.write(f"{functionId},{funcStat.coldStartTime},{funcStat.excutingTime},{funcStat.nColdStart},{funcStat.nExcution}\n")
        
    
    with open(f"./log/{policy}.csv", 'r') as f:
        lines = f.readlines()
        minMemoryReq = float(lines[0].strip().split(",")[1])
        time, coldStartTime, memorySize, excutingTime, nColdStart, nExcution = (
            lines[-1].strip().split(",")
        )
        print(f" Dataset: {dataset}")
        print(
            "Policy, ColdStartTime, MemorySize, ExcutingTime, NColdStart, NExcution, PeakMemory"
        )
        print(
            f"{policy}, {coldStartTime}, {memorySize}, {excutingTime}, {nColdStart}, {nExcution}, {minMemoryReq}"
        )