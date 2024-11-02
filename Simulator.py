from Include import *
from queue import PriorityQueue
from tqdm import tqdm
import heapq
import csv


class Stats:
    def __init__(
        self,
        time,
        coldStartTime: int = 0,
        memorySize: int = 0,
        excutingTime: int = 0,
        nColdStart: int = 0,
        nExcution: int = 0,
    ):
        self.time = time
        self.coldStartTime = coldStartTime
        self.memorySize = memorySize
        self.excutingTime = excutingTime
        self.nColdStart = nColdStart
        self.nExcution = nExcution


def log(msg: str, verbose: bool = True, filename: str = "log.txt", newfile: bool = False):
    if newfile:
        open(filename, "w").write("")
    if verbose:
        open(filename, "a").write(msg)


class Container:
    def __init__(self, priority: int, functionId: str, endTime: int):
        self.priority = priority
        self.functionId = functionId
        self.endTime = endTime
        self.frequency = 1

    def __lt__(self, other):
        return self.priority < other.priority


class Simulator:
    def __init__(
        self,
        memoryBudget: float,
        functionMap: dict[tuple, Function],
        invocationMap: dict[tuple, Invocation],
        policy: str = "LRU",
        timeLimit: int = 0,
        functionLimit: int = 0,
        logInterval: int = 1000,
    ):
        self.memoryBudget = memoryBudget  # MB
        self.functionMap = functionMap
        self.invocationMap = invocationMap
        self.policy = policy
        self.logInterval = logInterval
        self.TTL = min2ms(10)

        self.eventQueue = PriorityQueue()
        self.memoryUsed = 0
        self.cache: list[Container] = []
        self.stats: list[Stats] = []
        self.curMin = 0
        self.coldStartTime = 0
        self.excutingTime = 0
        self.lastLogTime = 0
        self.nColdStart = 0
        self.nExcution = 0
        self.minMemoryReq = 0

        # FassCache actually uses a logical clock instead of a physical clock
        # The clock is updated by the the priority of the envicted cache item
        self.logicalClock = 0

        with open(f"./log/{self.policy}.log", "w") as f:
            pass

        # init event queue
        functionCount = 0
        for functionId in tqdm(self.invocationMap):
            functionCount += 1
            if functionLimit and functionCount > functionLimit:
                break
            counts = self.invocationMap[functionId].Counts
            for min, count in enumerate(counts):
                if timeLimit and min > timeLimit:
                    break
                if count == 0:
                    continue
                for i in range(count):
                    time = min2ms(min + i / count)
                    self.eventQueue.put((time, functionId))

    def run(self):
        log(f"Policy {self.policy}\n", filename=f"./log/{self.policy}.log", newfile=True)
        log("Start simulation\n", newfile=True)
        for _ in tqdm(range(self.eventQueue.qsize())):
            self.process_event()
        print("Min memory requirement", self.minMemoryReq)

    def setPolicy(self, policy: str):
        self.policy = policy

    def getFreq(self, functionId):
        return sum([container.frequency for container in self.cache if container.functionId == functionId])

    def getPriority(self, time, functionId):
        freq = self.getFreq(functionId)
        # freq = self.invocationMap[functionId].Counts[int(ms2min(time))]
        cost = self.functionMap[functionId].coldStartTime
        size = self.functionMap[functionId].memory
        priority = time
        if self.policy == "LRU":
            priority = time
        elif self.policy == "LFU":
            priority = time + freq
        elif self.policy == "GD":
            priority = self.logicalClock + freq * (cost / size)
        elif self.policy == "FREQ":
            priority = self.logicalClock + freq * cost
        elif self.policy == "SIZE":
            priority = self.logicalClock + freq / size
        elif self.policy == "TTL":
            priority = time
        elif self.policy == "RAND":
            priority = np.random.randint(10)
        log(
            f"time {time}, clock {self.logicalClock}, functionId {functionId}, freq {freq}, cost {cost}, size {size}, priority {priority}\n",
            filename=f"./log/{self.policy}.log",
        )
        return priority

    def freeMemory(self, size, time):
        assert (
            self.memoryBudget >= size
        ), f"Memory budget too small, size {size}, memoryBudget {self.memoryBudget}"
        maxPriority = 0
        i = 0
        self.cache.sort(key=lambda x: x.priority)
        while self.memoryUsed + size > self.memoryBudget and i < len(self.cache):
            container = self.cache[i]
            # skip running container
            if container.endTime > time:
                i += 1
                continue
            # remove container
            self.memoryUsed -= self.functionMap[container.functionId].memory
            self.cache.pop(i)
            # update logical clock
            # maxPriority = max(maxPriority, container.priority)
            # self.logicalClock = maxPriority
            self.logicalClock = container.priority
        self.minMemoryReq = max(self.minMemoryReq, self.memoryUsed + size)
        if self.memoryUsed + size > self.memoryBudget:
            # TODO: should delay the event
            pass

    def newContainer(self, time, functionId):
        endTime = time + self.functionMap[functionId].duration + self.functionMap[functionId].coldStartTime
        priority = self.getPriority(time, functionId)
        self.cache.append(
            Container(priority, functionId, endTime)
        )
        self.memoryUsed += self.functionMap[functionId].memory
        return priority

    def findAvailContainer(self, time, functionId):
        # TTL policy
        if self.policy == "TTL":
            i = 0
            while i < len(self.cache):
                container = self.cache[i]
                if container.priority + self.TTL < time:
                    self.memoryUsed -= self.functionMap[container.functionId].memory
                    self.cache.pop(i)
                else:
                    i += 1
        # find available container and update
        for i,container in enumerate(self.cache):
            if container.functionId == functionId and container.endTime < time:
                self.cache[i].priority = self.getPriority(time, functionId)
                self.cache[i].endTime = time + self.functionMap[functionId].duration
                self.cache[i].frequency += 1
                return self.cache[i].priority
        # no available container
        return None

    def process_event(self):
        time, functionId = self.eventQueue.get()
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
        for i,container in enumerate(self.cache):
            if container.functionId == functionId:
                self.cache[i].priority = priority
        
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
            log(
                f"time {time}, coldStartTime {self.coldStartTime}, memoryUsed {self.memoryUsed}, excutingTime {self.excutingTime}, nColdStart {self.nColdStart}, nExcution {self.nExcution}\n",
                filename=f"./log/{self.policy}.log",
            )

    def dumpStats(self, location: str):
        csv_file = open(f"{location}/{self.policy}.csv", "w")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(
            [
                "time",
                "coldStartTime",
                "memorySize",
                "excutingTime",
                "nColdStart",
                "nExcution",
            ]
        )
        for stat in self.stats:
            csv_writer.writerow(
                [
                    stat.time,
                    stat.coldStartTime,
                    stat.memorySize,
                    stat.excutingTime,
                    stat.nColdStart,
                    stat.nExcution,
                ]
            )
