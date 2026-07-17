#Requires -Version 5.0
<#
.SYNOPSIS
    torx installer for Windows

.DESCRIPTION
    Downloads and installs the latest (or specified) torx release for Windows.

.PARAMETER Version
    Install a specific version tag (e.g. v0.2.0). Defaults to latest.

.PARAMETER InstallDir
    Custom install directory. Defaults to $env:LOCALAPPDATA\torx\bin

.EXAMPLE
    irm https://raw.githubusercontent.com/your-github-username/torx/main/install.ps1 | iex

.EXAMPLE
    & ([scriptblock]::Create((irm https://raw.githubusercontent.com/your-github-username/torx/main/install.ps1))) -Version v0.2.0
#>

param(
    [string]$Version = "",
    [string]$InstallDir = ""
)

$ErrorActionPreference = "Stop"

$Repo = "your-github-username/torx"
$Binary = "torx.exe"

function Write-Info    { param($msg) Write-Host "[torx] $msg" -ForegroundColor Cyan }
function Write-Success { param($msg) Write-Host "[torx] $msg" -ForegroundColor Green }
function Write-Warn    { param($msg) Write-Host "[torx] $msg" -ForegroundColor Yellow }
function Write-Err     { param($msg) Write-Host "[torx] error: $msg" -ForegroundColor Red; exit 1 }

# ─────────────────────────────────────────────
# detect architecture
# ─────────────────────────────────────────────
function Get-Platform {
    $arch = $env:PROCESSOR_ARCHITECTURE
    if ($env:PROCESSOR_ARCHITEW6432) { $arch = $env:PROCESSOR_ARCHITEW6432 }

    switch -Wildcard ($arch) {
        "AMD64" { return "x86_64-pc-windows-msvc" }
        "ARM64" { return "aarch64-pc-windows-msvc" }
        default { Write-Err "unsupported architecture: $arch" }
    }
}

# ─────────────────────────────────────────────
# fetch latest version from GitHub
# ─────────────────────────────────────────────
function Get-LatestVersion {
    if ($Version -ne "") {
        Write-Info "using specified version: $Version"
        return $Version
    }

    Write-Info "fetching latest release..."
    try {
        $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest"
        $ver = $release.tag_name
    } catch {
        Write-Err "could not fetch latest release. check https://github.com/$Repo/releases"
    }

    if ([string]::IsNullOrEmpty($ver)) {
        Write-Err "could not determine latest version"
    }

    Write-Info "latest version: $ver"
    return $ver
}

# ─────────────────────────────────────────────
# download and extract
# ─────────────────────────────────────────────
function Get-Binary {
    param($Platform, $Ver)

    $archive = "torx-$Ver-$Platform.zip"
    $url = "https://github.com/$Repo/releases/download/$Ver/$archive"
    $tmpDir = Join-Path $env:TEMP "torx-install-$(Get-Random)"
    New-Item -ItemType Directory -Path $tmpDir | Out-Null

    $archivePath = Join-Path $tmpDir $archive

    Write-Info "downloading $archive..."
    try {
        Invoke-WebRequest -Uri $url -OutFile $archivePath -UseBasicParsing
    } catch {
        Write-Err "download failed. check https://github.com/$Repo/releases"
    }

    Write-Info "extracting..."
    Expand-Archive -Path $archivePath -DestinationPath $tmpDir -Force

    $binaryPath = Get-ChildItem -Path $tmpDir -Recurse -Filter $Binary | Select-Object -First 1
    if (-not $binaryPath) {
        Write-Err "binary '$Binary' not found in archive"
    }

    return $binaryPath.FullName
}

# ─────────────────────────────────────────────
# install binary
# ─────────────────────────────────────────────
function Install-Binary {
    param($BinaryPath)

    $dir = if ($InstallDir -ne "") { $InstallDir } else { Join-Path $env:LOCALAPPDATA "torx\bin" }

    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }

    $target = Join-Path $dir $Binary
    Copy-Item -Path $BinaryPath -Destination $target -Force

    Write-Success "installed torx to $target"
    return $dir
}

# ─────────────────────────────────────────────
# add to PATH (user-level, persistent)
# ─────────────────────────────────────────────
function Add-ToPath {
    param($Dir)

    $currentPath = [Environment]::GetEnvironmentVariable("Path", "User")

    if ($currentPath -split ';' -contains $Dir) {
        Write-Success "torx is ready — run: torx"
        return
    }

    Write-Info "adding $Dir to your user PATH..."
    $newPath = "$currentPath;$Dir"
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")

    # update current session too so it works immediately
    $env:Path = "$env:Path;$Dir"

    Write-Success "added to PATH (restart your terminal for it to persist everywhere)"
}

# ─────────────────────────────────────────────
# verify
# ─────────────────────────────────────────────
function Test-Install {
    param($Dir)

    $target = Join-Path $Dir $Binary
    try {
        $out = & $target --version 2>&1
        Write-Success "verified: $out"
    } catch {
        Write-Warn "installed but could not verify — run 'torx --version' to check"
    }
}

# ─────────────────────────────────────────────
# main
# ─────────────────────────────────────────────
function Main {
    Write-Host ""
    Write-Host "torx - BitTorrent client installer" -ForegroundColor Cyan
    Write-Host "────────────────────────────────────────"
    Write-Host ""

    $platform = Get-Platform
    Write-Info "detected platform: $platform"

    $ver = Get-LatestVersion
    $binaryPath = Get-Binary -Platform $platform -Ver $ver
    $installDir = Install-Binary -BinaryPath $binaryPath
    Add-ToPath -Dir $installDir
    Test-Install -Dir $installDir

    Write-Host ""
    Write-Success "done! start downloading:"
    Write-Host "  torx <path/to/file.torrent>"
    Write-Host ""
}

Main