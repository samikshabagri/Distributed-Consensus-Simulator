@echo off
echo Compiling Raft Simulator...
g++ -std=c++14 Message.h ThreadSafeQueue.h NetworkSimulator.h RaftNode.cpp main.cpp -o raft_sim.exe -pthread
if %errorlevel% neq 0 (
    echo Compilation failed!
    pause
    exit /b %errorlevel%
)
echo Compilation successful. Running simulator:
echo.
raft_sim.exe
pause
