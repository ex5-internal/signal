@echo off
REM sinyald nobetci dongusunu GIZLI baslatir.
REM
REM Baslangic klasorune BUNUN kisayolu konur; oturum acilisinda Explorer
REM calistirir, boylece surec Explorer'in cocugu olur.
REM
REM NEDEN ONEMLI: gecici bir kabuktan baslatilan surec, o kabugun Windows
REM is nesnesi (Job Object) KILL_ON_JOB_CLOSE ile kuruluysa kabuk kapaninca
REM IZSIZ olur -- ne panik, ne olay kaydi. 14-15 Agustos 2026'da daemon iki
REM kez boyle oldu. Explorer kalici oldugu icin bu tuzaga dusmez.

start "" /min "C:\Program Files\PowerShell\7\pwsh.exe" -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "D:\Projeler\Sinyal\tools\sinyald-nobet-dongu.ps1"
