import numpy as np
import csv 
from Include import *

def load_data(datasetPath:str):
    # datasetPath="/Users/jiaruiye/Code/Serverless/dataset"
    durationFile=f"{datasetPath}/function_durations_percentiles.anon.d01.csv"
    memoryFile=f"{datasetPath}/app_memory_percentiles.anon.d01.csv"
    invocationFile=f"{datasetPath}/invocations_per_function_md.anon.d01.csv"
    durationData = None
    memoryData = None
    invocationData = None
    try:
        with open(durationFile, 'r') as f:
            reader = csv.reader(f)
            durationData = list(reader)
    except IOError:
        print("Could not read file: ", durationFile)
    try:
        with open(memoryFile, 'r') as f:
            reader = csv.reader(f)
            memoryData = list(reader)
    except IOError:
        print("Could not read file: ", memoryFile)
    try:
        with open(invocationFile, 'r') as f:
            reader = csv.reader(f)
            invocationData = list(reader)
    except IOError:
        print("Could not read file: ", invocationFile)
    assert durationData and memoryData and invocationData, "Data not loaded"
    
    # key: (HashOwner, HashApp)
    durationMap = {}
    memoryMap = {}
    invocationMap = {}
    
    for line in durationData[1:]:
        HashOwner = line[0]
        HashApp = line[1]
        HashFunction = line[2]
        Average = int(line[3])
        Count = int(line[4])
        Minimum = float(line[5])
        Maximum = float(line[6])
        if (HashOwner, HashApp) not in durationMap:
            durationMap[(HashOwner, HashApp)] = {}
        if HashFunction not in durationMap[(HashOwner, HashApp)]:
            durationMap[(HashOwner, HashApp)][HashFunction] = Duration(HashOwner, HashApp, HashFunction, Average, Count, Minimum, Maximum)
    for line in memoryData[1:]:
        HashOwner = line[0]
        HashApp = line[1]
        SampleCount = int(line[2])
        AverageAllocatedMb = int(line[3])
        if (HashOwner, HashApp) not in memoryMap:
            memoryMap[(HashOwner, HashApp)] = Memory(HashOwner, HashApp, SampleCount, AverageAllocatedMb)

    # generate function data
    functionMap = {}
    for (HashOwner, HashApp) in durationMap:
        for HashFunction in durationMap[(HashOwner, HashApp)]:
            duration:Duration = durationMap[(HashOwner, HashApp)][HashFunction]
            durationTime = duration.Average
            coldStartTime = duration.Maximum-durationTime
            if (HashOwner, HashApp) not in memoryMap:
                continue
            memory:Memory = memoryMap[(HashOwner, HashApp)]
            count = len(durationMap[(HashOwner, HashApp)])
            functionMemory = memory.AverageAllocatedMb/count
            if (HashOwner, HashApp, HashFunction) not in functionMap:
                functionMap[(HashOwner, HashApp, HashFunction)] = Function(HashOwner, HashApp, HashFunction, coldStartTime, durationTime, functionMemory)
                
    for line in invocationData[1:]:
        HashOwner = line[0]
        HashApp = line[1]
        HashFunction = line[2]
        Trigger = line[3]
        Counts = list(map(int, line[4:]))
        if (HashOwner, HashApp, HashFunction) not in invocationMap and (HashOwner, HashApp, HashFunction) in functionMap:
            invocationMap[(HashOwner, HashApp, HashFunction)] = Invocation(HashOwner, HashApp, HashFunction, Trigger, Counts)
    print("Data loaded successfully")
    
    return functionMap, invocationMap
    
    
if __name__ == "__main__":
    functionMap, invocationMap = load_data()
    for i,(HashOwner, HashApp, HashFunction) in enumerate(functionMap):
        if i>10:
            break
        function = functionMap[(HashOwner, HashApp, HashFunction)]
        print(f"Function {i}: {function.HashOwner}, {function.HashApp}, {function.HashFunction}, {function.coldStartTime}, {function.duration}, {function.memory}")