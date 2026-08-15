@echo off
call "C:/Program Files/Microsoft Visual Studio/2022/Enterprise/VC/Auxiliary/Build/vcvars64.bat"
echo [TEST] where cl.exe:
where cl.exe
echo [TEST] cl version:
cl.exe 2>&1 | findstr /i "Version"
echo [TEST] DONE
