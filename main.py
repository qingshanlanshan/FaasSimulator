from Include import *
from Simulator import *
from TraceGen import *
from multiprocessing import Pool
from dataclasses import dataclass


def runSimulation(
    memorySize,
    functionMap,
    invocationMap,
    policy,
    timeLimit,
    functionLimit,
    logInterval,
):
    if policy == "Baseline":
        memorySize = 1e7
    simulator = Simulator(
        memorySize,
        functionMap,
        invocationMap,
        policy,
        timeLimit,
        functionLimit,
        logInterval,
        True,
    )
    simulator.run()
    simulator.dumpStats(f"./log")


@dataclass
class Settings:
    memoryBudget: float
    timeLimit: int
    functionLimit: int


if __name__ == "__main__":
    day = 1
    dataLocation = "/home/jiarui/Serverless/dataset"
    policies = [
        "TTL",
        "LRU",
        "LFU",
        "GD",
        "FREQCOST",
        "FREQSIZE",
        "COSTSIZE",
        "RAND",
        "Baseline",
    ]

    # in min
    timeLimit = 100
    # in ms
    logInterval = 1e3
    # modify memory budget based on the actual dataset generated
    settings = {
        "Representative": Settings(
            timeLimit=timeLimit,
            functionLimit=400,
            memoryBudget=17e3,
        ),
        "Rare": Settings(
            timeLimit=timeLimit,
            functionLimit=1000,
            memoryBudget=5e3,
        ),
        "Random": Settings(
            timeLimit=timeLimit,
            functionLimit=400,
            memoryBudget=1.2e5,
        ),
    }

    for dataset, setting in settings.items():
        functionMap, invocationMap = load_data(
            "/home/jiarui/Serverless/dataset", 1, dataset
        )
        print(f"Memory budget: {setting.memoryBudget}")
        p = Pool(len(policies))

        for policy in policies:
            p.apply_async(
                runSimulation,
                args=(
                    setting.memoryBudget,
                    functionMap,
                    invocationMap,
                    policy,
                    setting.timeLimit,
                    setting.functionLimit,
                    logInterval,
                ),
            )
        p.close()
        p.join()
        l = []
        for policy in policies:
            # summary
            with open(f"log/{policy}.csv", "r") as f:
                lines = f.readlines()
                minMemoryReq = float(lines[0].split(",")[1])
                time, coldStartTime, memorySize, excutingTime, nColdStart, nExcution = (
                    lines[-1].split(",")
                )
                l.append(
                    [
                        policy,
                        coldStartTime,
                        memorySize,
                        excutingTime,
                        nColdStart,
                        nExcution,
                        minMemoryReq,
                    ]
                )
        baseline = l[-1]
        baselineExcutionTime = float(baseline[3])
        l.sort(key=lambda x: float(x[1]))
        print(f" \nDataset: {dataset}")
        print("Policy, %TimeIncrease, %ColdStart, PeakMemory")
        for _ in l:
            (
                policy,
                coldStartTime,
                memorySize,
                excutingTime,
                nColdStart,
                nExcution,
                minMemoryReq,
            ) = _
            print(
                f"{policy}, {100*(float(excutingTime) - baselineExcutionTime)/baselineExcutionTime}, {100*float(nColdStart)/float(nExcution)}, {minMemoryReq}"
            )
