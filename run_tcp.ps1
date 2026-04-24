param(
  [string]$ConfigPath = ".\config.sexp",
  [string]$ServerHost = "127.0.0.1",
  [int]$Port = 7777,
  [string[]]$Command,
  [string]$CommandsPath,
  [switch]$UseExistingServer,
  [bool]$UseOpamInstalledServer = $true,
  [switch]$SkipBuild,
  [switch]$NoInteractive
)

$ErrorActionPreference = "Stop"

function Invoke-Build {
  Write-Host "Building Sakura..."
  dune build
  if (-not $?) {
    throw "Build failed."
  }
}

function Start-SakuraServer {
  param(
    [string]$Config,
    [string]$WorkingDirectory,
    [switch]$UseOpam
  )

  Write-Host "Starting Sakura server with config $Config ..."
  $filePath = "dune"
  $args = @("exec", "sakura-server", "--", $Config)
  if ($UseOpam) {
    $filePath = "opam"
    $args = @("exec", "--", "sakura-server", $Config)
  }

  $proc = Start-Process -FilePath $filePath `
    -ArgumentList $args `
    -WorkingDirectory $WorkingDirectory `
    -PassThru

  return $proc
}

function Wait-ForTcp {
  param(
    [string]$TargetHost,
    [int]$TargetPort,
    [int]$TimeoutSeconds = 20
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    $client = $null
    try {
      $client = [System.Net.Sockets.TcpClient]::new()
      $async = $client.BeginConnect($TargetHost, $TargetPort, $null, $null)
      if ($async.AsyncWaitHandle.WaitOne(250)) {
        $client.EndConnect($async)
        $client.Close()
        return
      }
      $client.Close()
    } catch {
      if ($null -ne $client) {
        $client.Close()
      }
      Start-Sleep -Milliseconds 250
    }
  }

  throw "Timed out waiting for TCP server at $TargetHost`:$TargetPort"
}

function Connect-Tcp {
  param(
    [string]$TargetHost,
    [int]$TargetPort
  )

  $client = [System.Net.Sockets.TcpClient]::new($TargetHost, $TargetPort)
  $stream = $client.GetStream()
  $writer = [System.IO.StreamWriter]::new($stream)
  $writer.AutoFlush = $true
  $reader = [System.IO.StreamReader]::new($stream)

  return @{
    Client = $client
    Writer = $writer
    Reader = $reader
  }
}

function Send-Command {
  param(
    [System.IO.StreamWriter]$Writer,
    [System.IO.StreamReader]$Reader,
    [string]$Line
  )

  $Writer.WriteLine($Line)
  $response = $Reader.ReadLine()
  if ($null -eq $response) {
    throw "Connection closed by server."
  }

  Write-Host "< $response"
}

$repoRoot = $PSScriptRoot
$serverProcess = $null

try {
  if (-not $SkipBuild) {
    Invoke-Build
  }

  if (-not $UseExistingServer) {
    if ($UseOpamInstalledServer) {
      Write-Host "Server mode: opam-installed sakura-server"
    } else {
      Write-Host "Server mode: local dune exec sakura-server"
    }
    $serverProcess = Start-SakuraServer -Config $ConfigPath -WorkingDirectory $repoRoot -UseOpam:$UseOpamInstalledServer
    Wait-ForTcp -TargetHost $ServerHost -TargetPort $Port
    Write-Host "Server is ready on $ServerHost`:$Port"
  }

  $conn = Connect-Tcp -TargetHost $ServerHost -TargetPort $Port
  $writer = $conn.Writer
  $reader = $conn.Reader

  $commandsToSend = @()
  if ($null -ne $Command -and $Command.Count -gt 0) {
    $commandsToSend += $Command
  }
  if (-not [string]::IsNullOrWhiteSpace($CommandsPath)) {
    $fileCommands = Get-Content -Path $CommandsPath |
      Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    $commandsToSend += $fileCommands
  }

  if ($commandsToSend.Count -gt 0) {
    foreach ($cmd in $commandsToSend) {
      Write-Host "> $cmd"
      Send-Command -Writer $writer -Reader $reader -Line $cmd
    }
  }

  if (-not $NoInteractive) {
    Write-Host "Interactive mode. Type :quit to exit."
    while ($true) {
      $line = Read-Host ">"
      if ($line -eq ":quit") {
        break
      }
      if ([string]::IsNullOrWhiteSpace($line)) {
        continue
      }

      Send-Command -Writer $writer -Reader $reader -Line $line
    }
  }

  $writer.Dispose()
  $reader.Dispose()
  $conn.Client.Close()
}
finally {
  if ($null -ne $serverProcess -and -not $serverProcess.HasExited) {
    Write-Host "Stopping Sakura server (PID $($serverProcess.Id))"
    Stop-Process -Id $serverProcess.Id -Force
  }
}
