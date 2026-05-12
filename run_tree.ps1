param(
    [switch]$NoRun
)

$ErrorActionPreference = "Stop"

$repoRoot = $PSScriptRoot
$projectName = Split-Path -Leaf $repoRoot

Push-Location $repoRoot
try {
    (& opam env) -split '\r?\n' | ForEach-Object {
        if ($_ -ne "") {
            Invoke-Expression $_
        }
    }

    dune build shared/shabptree/sakura_shabptree.dll bin/tree.exe

    $dll = @(
        Get-ChildItem -Path $repoRoot -Filter sakura_shabptree.dll -Recurse -ErrorAction SilentlyContinue
        Get-ChildItem -Path (Join-Path $repoRoot "..") -Filter sakura_shabptree.dll -Recurse -ErrorAction SilentlyContinue |
            Where-Object { $_.FullName -like "*\_build\*\$projectName\shared\shabptree\sakura_shabptree.dll" }
    ) | Select-Object -First 1

    if ($null -eq $dll) {
        throw "Could not find built sakura_shabptree.dll"
    }

    $env:SAKURA_BPLUSTREE_DLL = $dll.FullName

    if (-not $NoRun) {
        dune exec bin/tree.exe
    }
}
finally {
    Pop-Location
}
