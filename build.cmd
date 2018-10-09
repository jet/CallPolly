dotnet pack src/CallPolly --configuration Release -o "%CD%\bin" --version-suffix CI%1
if ERRORLEVEL 1 (echo Error building CallPolly; exit /b 1)

dotnet test tests/CallPolly.Tests --configuration Release
if ERRORLEVEL 1 (echo Error testing CallPolly; exit /b 1)