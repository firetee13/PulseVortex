Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = WshShell.CurrentDirectory
WshShell.Run "pyenv exec pythonw run_monitor_gui.pyw", 0, False
Set WshShell = Nothing