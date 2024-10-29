import numpy as np

class Duration:
    def __init__(self, HashOwner, HashApp, HashFunction, Average, Count, Minimum, Maximum):
        self.HashOwner = HashOwner
        self.HashApp = HashApp
        self.HashFunction = HashFunction
        self.Average = Average
        self.Count = Count
        self.Minimum = Minimum
        self.Maximum = Maximum
# HashOwner,HashApp,SampleCount,AverageAllocatedMb
class Memory:
    def __init__(self, HashOwner, HashApp, SampleCount, AverageAllocatedMb):
        self.HashOwner = HashOwner
        self.HashApp = HashApp
        self.SampleCount = SampleCount
        self.AverageAllocatedMb = AverageAllocatedMb
        
# HashOwner,HashApp,HashFunction,Trigger,1..1440
class Invocation:
    def __init__(self, HashOwner, HashApp, HashFunction, Trigger:str, Counts:list[int]):
        self.HashOwner = HashOwner
        self.HashApp = HashApp
        self.HashFunction = HashFunction
        self.Trigger = Trigger
        self.Counts = Counts

class Function:
    def __init__(self, HashOwner, HashApp, HashFunction, coldStartTime, duration, memory):
        self.HashOwner = HashOwner
        self.HashApp = HashApp
        self.HashFunction = HashFunction
        self.coldStartTime = coldStartTime
        self.duration = duration
        self.memory = memory
        
def min2ms(minute:float)->float:
    return minute*60*1000
def ms2min(ms:float)->float:
    return ms/(60*1000)