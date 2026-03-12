# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_submodules

hiddenimports = []
hiddenimports += collect_submodules("web.routes")

a = Analysis(
    ["bazaar_chronicle.py"],
    pathex=[],
    binaries=[],
    datas=[
        ("web/templates", "web/templates"),
        ("web/static", "web/static"),
        ("resources", "resources"),
    ],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "tkinter",
        "unittest",
        "test",
        "matplotlib",
        "pandas",
        "scipy",
        "IPython",
        "jupyter",
        "cv2.qt",
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="BazaarChronicle",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    version="version.txt",
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="BazaarChronicle",
)
