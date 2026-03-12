@echo off
echo ====================================
echo BazaarChronicle Release Build
echo ====================================
echo.

echo Cleaning previous builds...
rmdir /s /q build 2>nul
rmdir /s /q dist 2>nul

echo.
echo Running PyInstaller...
pyinstaller --clean --noconfirm BazaarChronicle.spec

echo.
echo Build finished!
echo Output folder:
echo dist\BazaarChronicle
echo.

pause
