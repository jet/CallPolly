SETLOCAL
SET X=%1
SET BLD=000000%X%
SET BLD=%BLD:~-7%
if [%2]==[] (SET V=CI%BLD%) else (SET V=pr%2-%BLD%)

dotnet pack src/CallPolly --configuration Release -o "%CD%\bin" --version-suffix %V%
if ERRORLEVEL 1 (echo Error building CallPolly; exit /b 1)

dotnet test tests/CallPolly.Tests --configuration Release
if ERRORLEVEL 1 (echo Error testing CallPolly; exit /b 1)

dotnet test tests/CallPolly.Acceptance --configuration Release
if ERRORLEVEL 1 (echo Error acceptance testing CallPolly; exit /b 1)

ENDLOCAL
