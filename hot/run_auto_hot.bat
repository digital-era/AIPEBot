@echo on

rem 切换控制台代码页到 UTF-8
chcp 65001 >nul

rem 让 Python 的 stdout 用 UTF-8
set PYTHONIOENCODING=utf-8

rem ★ 新增：禁用输出缓冲，日志实时写入文件
set PYTHONUNBUFFERED=1

cd E:\AIPEBot\hot

python E:\AIPEBot\hot\auto_hot.py > E:\AIPEBot\hot\auto_hot.log 2>&1
set RC=%ERRORLEVEL%

rem ── 结果判定 0 成功 2 生成无效 1 其他异常 ──
if %RC%==0 (
    echo [%date% %time%] 任务成功 >> E:\AIPEBot\hot\task_status.log
) else (
    echo [%date% %time%] 任务失败，退出码 %RC% >> E:\AIPEBot\hot\task_status.log
    rem ★ 在这里放失败时的动作，例如弹窗、发通知等：
    rem powershell -Command "New-BurntToastNotification -Text '热点图谱生成失败'"
)

exit /b %RC%

