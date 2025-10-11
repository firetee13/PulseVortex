Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = WshShell.CurrentDirectory
' Launch the monitor GUI using the CLI entry point
WshShell.Run "pyenv exec monitor-gui", 0, False
Set WshShell = Nothing
