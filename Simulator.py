from Include import *
from queue import PriorityQueue
from tqdm import tqdm
import heapq
import csv

class Stats:
    def __init__(self, time, coldStartTime:int=0, memorySize:int=0, excutingTime:int=0):
        self.time = time
        self.coldStartTime = coldStartTime
        self.memorySize = memorySize
        self.excutingTime = excutingTime


class Simulator:
    def __init__(self, memoryBudget:float, functionMap:dict[tuple,Function], invocationMap:dict[tuple,Invocation], policy:str="LRU", timeLimit:int=0, functionLimit:int=0):
        self.memoryBudget = memoryBudget # MB
        self.functionMap = functionMap
        self.invocationMap = invocationMap
        self.policy = policy
        
        self.eventQueue = PriorityQueue()
        self.memoryUsed = 0
        self.cache = []
        self.stats:list[Stats] = []
        self.curMin = 0
        self.coldStartTime = 0
        self.excutingTime = 0
        
        # init event queue
        functionCount=0
        for functionId in tqdm(self.invocationMap):
            functionCount+=1
            if functionLimit and functionCount>functionLimit:
                break
            counts = self.invocationMap[functionId].Counts
            for min,count in enumerate(counts):
                if timeLimit and min>=timeLimit:
                    break
                if count == 0:
                    continue
                for i in range(count):
                    time = min2ms(min+i/count)
                    self.eventQueue.put((time, functionId))

    def run(self):
        for _ in tqdm(range(self.eventQueue.qsize())):
            self.process_event()
    
    def setPolicy(self, policy:str):
        self.policy = policy
    
    def getPriority(self, time, functionId):
        if self.policy == "LRU":
            return time
        elif self.policy == "LFU":
            # hack: use the frequency of the current minute
            return self.invocationMap[functionId].Counts[int(ms2min(time))]
        elif self.policy == "GD":
            freq = self.invocationMap[functionId].Counts[int(ms2min(time))]
            # cost = cold start time
            cost = self.functionMap[functionId].coldStartTime
            size=self.functionMap[functionId].memory
            return time + freq*cost/size
        elif self.policy == "FREQ":
            freq = self.invocationMap[functionId].Counts[int(ms2min(time))]
            cost = self.functionMap[functionId].coldStartTime
            return time+freq*cost
        elif self.policy == "SIZE":
            cost = self.functionMap[functionId].coldStartTime
            size=self.functionMap[functionId].memory
            return time+cost/size
        elif self.policy == "RAND":
            return np.random.randint(10)
        return time
            
    def freeCache(self, size):
        assert self.memoryBudget>=size, "Memory budget too small"
        while self.memoryUsed+size>self.memoryBudget:
            priority, functionId = heapq.heappop(self.cache)
            functionInfo = self.functionMap[functionId]
            self.memoryUsed-=functionInfo.memory
    
    def addCache(self, time, functionId):
        assert self.memoryUsed+self.functionMap[functionId].memory<=self.memoryBudget, "Memory not enough"
        # self.cache.put((self.getPriority(time, functionId), functionId))
        heapq.heappush(self.cache, (self.getPriority(time, functionId), functionId))
        self.memoryUsed+=self.functionMap[functionId].memory
        
    def updateCache(self, time, functionId):
        # linear search, can be optimized by hash table or bloom filter
        for i, (priority, function) in enumerate(self.cache):
            if function == functionId:
                self.cache[i] = (self.getPriority(time, functionId), function)
                heapq.heapify(self.cache)
                return True
        return False
        
    def process_event(self):
        time, functionId = self.eventQueue.get()
        functionInfo = self.functionMap[functionId]
        coldStartTime = 0
        excutingTime = functionInfo.duration
        success = self.updateCache(time, functionId)
        if not success:
            coldStartTime = functionInfo.coldStartTime
            excutingTime += coldStartTime
            self.freeCache(functionInfo.memory)
            self.addCache(time,functionId)
        
        # stats
        self.coldStartTime+=coldStartTime
        self.excutingTime+=excutingTime
        curMin = int(ms2min(time))
        if curMin != self.curMin:
            self.curMin = curMin
            self.stats.append(Stats(curMin, self.coldStartTime, self.memoryUsed, self.excutingTime))

    def dumpStats(self,location:str):
        csv_file = open(f"{location}/{self.policy}.csv", "w")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["time", "coldStartTime", "memorySize", "excutingTime"])
        for stat in self.stats:
            csv_writer.writerow([stat.time, stat.coldStartTime, stat.memorySize, stat.excutingTime])