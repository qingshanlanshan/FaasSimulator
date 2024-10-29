from Include import *
from queue import PriorityQueue
from tqdm import tqdm
import heapq

class Stats:
    def __init__(self, time, coldStartTime:int=0, memorySize:int=0, excutingTime:int=0):
        self.time = time
        self.coldStartTime = coldStartTime
        self.memorySize = memorySize
        self.excutingTime = excutingTime


class Simulator:
    def __init__(self, memoryBudget:float, functionMap:dict[tuple,Function], invocationMap:dict[tuple,Invocation], policy:str="FIFO", timeLimit:int=0, functionLimit:int=0):
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
    
    
    def getPriority(self, time, functionId):
        if self.policy=="FIFO":
            return time
        elif self.eviction_policy == "RAND":
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
