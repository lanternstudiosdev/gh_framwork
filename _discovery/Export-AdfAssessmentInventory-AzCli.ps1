<#
Export-AdfAssessmentInventory-AzCli.ps1

ADF assessment inventory + FULL lineage graph using Azure CLI only.
- No Az PowerShell modules
- No jq
- Works in Windows PowerShell 5.1 (no ConvertFrom-Json -Depth)

Outputs:
  1) Inventory CSV (consolidated)
  2) Lineage CSV (edge list)
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory)] [string] $InputCsv,
  [Parameter(Mandatory)] [string] $InventoryCsv,
  [Parameter(Mandatory)] [string] $LineageCsv,
  [Parameter()] [string] $ErrorLog = (Join-Path (Split-Path $InventoryCsv -Parent) 'adf_inventory_errors.log'),
  [Parameter()] [switch] $IncludePropertiesJson,
  [Parameter()] [int] $ThrottleDelayMs = 2000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Assert-Cli {
  try { & az --version | Out-Null }
  catch { throw 'Azure CLI (az) not found on PATH.' }
}

function AzJson {
  param([Parameter(Mandatory)][string[]] $CliArgs)
  $maxRetries = 3
  for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
    # az on Windows is az.cmd (a batch file), so cmd.exe parses the arguments.
    # PowerShell's & operator doesn't quote args that lack spaces, so bare & in
    # nextLink URLs (e.g. …&$skiptoken=…) gets treated as a command separator.
    # Fix: use System.Diagnostics.Process to construct the argument string
    # ourselves, explicitly quoting every argument so cmd.exe sees & as literal.
    $allArgs = @($CliArgs) + @('--only-show-errors','-o','json')
    $argLine = ($allArgs | ForEach-Object { '"{0}"' -f $_ }) -join ' '

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName               = 'cmd.exe'
    $psi.Arguments              = "/c az $argLine"
    $psi.UseShellExecute        = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true
    $psi.CreateNoWindow         = $true

    $proc   = [System.Diagnostics.Process]::Start($psi)
    $stdout = $proc.StandardOutput.ReadToEnd()
    $stderr = $proc.StandardError.ReadToEnd()
    $proc.WaitForExit()

    $exitCode = $proc.ExitCode
    $rawStr   = $stdout.Trim()

    if ($exitCode -ne 0) {
      $errText = if ($stderr) { $stderr.Trim() } else { $rawStr }
      # az rest sometimes returns exit code 1 even with valid JSON — try parsing first
      if (-not [string]::IsNullOrWhiteSpace($rawStr) -and ($rawStr.StartsWith('{') -or $rawStr.StartsWith('['))) {
        try { return $rawStr | ConvertFrom-Json } catch {}
      }
      # Detect ARM throttling (429) and retry with exponential back-off
      if ($errText -match '429' -or $errText -match 'Too Many Requests' -or $errText -match 'throttl') {
        $wait = [math]::Pow(2, $attempt) * 5
        Write-Host ("  ARM throttle detected (attempt $attempt/$maxRetries). Waiting ${wait}s...") -ForegroundColor Yellow
        Start-Sleep -Seconds $wait
        continue
      }
      throw "az exited with code ${exitCode}: $errText"
    }
    if ([string]::IsNullOrWhiteSpace($rawStr)) { return $null }
    return $rawStr | ConvertFrom-Json
  }
  throw "az call failed after $maxRetries retries due to throttling: $CliArgs"
}

function AzJsonPaged {
  # Follows nextLink pagination. Returns all items from a list endpoint.
  param([Parameter(Mandatory)][string] $Url)
  $allItems = @()
  $currentUrl = $Url
  while ($currentUrl) {
    $resp = AzJson @('rest','--method','get','--url',$currentUrl)
    if ($resp) {
      if ($resp.PSObject.Properties.Name -contains 'value') {
        $allItems += @($resp.value)
      }
      if ($resp.PSObject.Properties.Name -contains 'nextLink' -and $resp.nextLink) {
        $currentUrl = $resp.nextLink
      } else {
        $currentUrl = $null
      }
    } else {
      $currentUrl = $null
    }
  }
  return $allItems
}

function Get-ValueArray {
  param([object] $Resp)
  if ($Resp -and ($Resp.PSObject.Properties.Name -contains 'value')) { return @($Resp.value) }
  return @()
}

function Get-SafeProp {
  param([object] $Obj, [string] $Name)
  if ($Obj -and ($Obj.PSObject.Properties.Name -contains $Name)) { return $Obj.$Name }
  return $null
}

function To-CompactJson {
  param([object] $Obj)
  if (-not $Obj) { return $null }
  try { return ($Obj | ConvertTo-Json -Depth 200 -Compress) } catch { return $null }
}

function Add-InvRow {
  param(
    [System.Collections.Generic.List[object]] $List,
    [string] $Sub,
    [string] $Rg,
    [string] $Adf,
    [string] $Cat,
    [string] $Name,
    [string] $Type,
    [string] $Rel = '',
    [string] $Props = $null
  )

  $List.Add([pscustomobject]@{
    SubscriptionId    = $Sub.Trim().ToUpper()
    ResourceGroupName = $Rg.Trim().ToUpper()
    DataFactoryName   = $Adf.Trim().ToUpper()
    ArtifactCategory  = $Cat.Trim()
    ArtifactName      = $Name.Trim()
    ArtifactType      = $Type
    RelatedArtifacts  = $Rel
    PropertiesJson    = $Props
  }) | Out-Null
}

function Add-Edge {
  param(
    [System.Collections.Generic.List[object]] $Edges,
    [string] $Sub,
    [string] $Rg,
    [string] $Adf,
    [string] $SrcType,
    [string] $SrcName,
    [string] $RelType,
    [string] $TgtType,
    [string] $TgtName,
    [string] $Notes = '',
    [string] $Pipeline = ''
  )

  if ([string]::IsNullOrWhiteSpace($TgtName)) { return }

  $srcLocalId = if ($SrcType -eq 'Activity' -and $Pipeline) { "${Pipeline}::${SrcName}" } else { $SrcName }
  $tgtLocalId = if ($TgtType -eq 'Activity' -and $Pipeline) { "${Pipeline}::${TgtName}" } else { $TgtName }

  # Build fully-qualified, sanitized NodeIds: SUB|RG|ADF|TYPE|LOCALID (uppercased, trimmed, non-printables removed)
  $srcNodeId = (@($Sub, $Rg, $Adf, $SrcType, $srcLocalId) | ForEach-Object { $_.Trim() }) -join '|'
  $srcNodeId = $srcNodeId.ToUpper() -replace '[^\x20-\x7E]', ''
  $tgtNodeId = (@($Sub, $Rg, $Adf, $TgtType, $tgtLocalId) | ForEach-Object { $_.Trim() }) -join '|'
  $tgtNodeId = $tgtNodeId.ToUpper() -replace '[^\x20-\x7E]', ''

  $Edges.Add([pscustomobject]@{
    SubscriptionId    = $Sub.Trim().ToUpper()
    ResourceGroupName = $Rg.Trim().ToUpper()
    DataFactoryName   = $Adf.Trim().ToUpper()
    SourceType        = $SrcType.Trim().ToUpper()
    SourceNodeId      = $srcNodeId
    SourceName        = $SrcName.Trim()
    RelationshipType  = $RelType.Trim().ToUpper()
    TargetType        = $TgtType.Trim().ToUpper()
    TargetNodeId      = $tgtNodeId
    TargetName        = $TgtName.Trim()
    Notes             = $Notes
  }) | Out-Null
}

function Get-ActivityChildren {
  param([object] $A)
  $kids = @()

  foreach ($p in @('activities','ifTrueActivities','ifFalseActivities','defaultActivities','elseActivities')) {
    if ($A.PSObject.Properties.Name -contains $p) { $kids += @($A.$p) }
  }

  if ($A.PSObject.Properties.Name -contains 'cases') {
    foreach ($c in @($A.cases)) {
      if ($c -and ($c.PSObject.Properties.Name -contains 'activities')) { $kids += @($c.activities) }
    }
  }

  return $kids
}

function Emit-DependencyEdges {
  param(
    [System.Collections.Generic.List[object]] $Edges,
    [string] $Sub,
    [string] $Rg,
    [string] $Adf,
    [string] $PipelineName,
    [object] $Activity
  )

  if ($Activity.PSObject.Properties.Name -contains 'dependsOn') {
    foreach ($d in @($Activity.dependsOn)) {
      if (-not $d) { continue }
      $src = Get-SafeProp $d 'activity'
      $conds = ''
      if ($d.PSObject.Properties.Name -contains 'dependencyConditions') {
        $conds = (@($d.dependencyConditions) -join '|')
      }
      if (-not [string]::IsNullOrWhiteSpace($src)) {
        $note = $(if ($conds) { "Conditions=$conds" } else { '' })
        Add-Edge $Edges $Sub $Rg $Adf 'Activity' $src 'PRECEDES' 'Activity' $Activity.name $note -Pipeline $PipelineName
        Add-Edge $Edges $Sub $Rg $Adf 'Activity' $Activity.name 'DEPENDS_ON' 'Activity' $src $note -Pipeline $PipelineName
      }
    }
  }
}

function Walk-Activities {
  param(
    [System.Collections.Generic.List[object]] $Edges,
    [System.Collections.Generic.List[object]] $Inv,
    [string] $Sub,
    [string] $Rg,
    [string] $Adf,
    [string] $PipelineName,
    [object[]] $Activities,
    [hashtable] $DsToLs,
    [hashtable] $LsToIr,
    [switch] $IncludeActivityInventory
  )

  foreach ($a in @($Activities)) {
    if (-not $a) { continue }

    $actName = $a.name
    $actType = $a.type

    if ($IncludeActivityInventory) {
      Add-InvRow $Inv $Sub $Rg $Adf 'Activity' $actName $actType ("Pipeline=$PipelineName") $(if ($IncludePropertiesJson) { To-CompactJson $a } else { $null })
    }

    Add-Edge $Edges $Sub $Rg $Adf 'Pipeline' $PipelineName 'CONTAINS' 'Activity' $actName ("ActivityType=$actType") -Pipeline $PipelineName

    Emit-DependencyEdges -Edges $Edges -Sub $Sub -Rg $Rg -Adf $Adf -PipelineName $PipelineName -Activity $a

    if ($a.PSObject.Properties.Name -contains 'inputs') {
    foreach ($i in @($a.inputs)) {
      if (-not $i) { continue }
      $ds = Get-SafeProp $i 'referenceName'
      Add-Edge $Edges $Sub $Rg $Adf 'Activity' $actName 'READS' 'Dataset' $ds '' -Pipeline $PipelineName

      if ($ds -and $DsToLs.ContainsKey($ds)) {
        $ls = $DsToLs[$ds]
        Add-Edge $Edges $Sub $Rg $Adf 'Dataset' $ds 'USES' 'LinkedService' $ls
        if ($ls -and $LsToIr.ContainsKey($ls)) {
          Add-Edge $Edges $Sub $Rg $Adf 'LinkedService' $ls 'RUNS_ON' 'IntegrationRuntime' $LsToIr[$ls]
        }
      }
    }
    }

    if ($a.PSObject.Properties.Name -contains 'outputs') {
    foreach ($o in @($a.outputs)) {
      if (-not $o) { continue }
      $ds = Get-SafeProp $o 'referenceName'
      Add-Edge $Edges $Sub $Rg $Adf 'Activity' $actName 'WRITES' 'Dataset' $ds '' -Pipeline $PipelineName

      if ($ds -and $DsToLs.ContainsKey($ds)) {
        $ls = $DsToLs[$ds]
        Add-Edge $Edges $Sub $Rg $Adf 'Dataset' $ds 'USES' 'LinkedService' $ls
        if ($ls -and $LsToIr.ContainsKey($ls)) {
          Add-Edge $Edges $Sub $Rg $Adf 'LinkedService' $ls 'RUNS_ON' 'IntegrationRuntime' $LsToIr[$ls]
        }
      }
    }
    }

    if ($a.PSObject.Properties.Name -contains 'linkedServiceName') {
      $ls = Get-SafeProp $a.linkedServiceName 'referenceName'
      Add-Edge $Edges $Sub $Rg $Adf 'Activity' $actName 'USES' 'LinkedService' $ls '' -Pipeline $PipelineName
      if ($ls -and $LsToIr.ContainsKey($ls)) {
        Add-Edge $Edges $Sub $Rg $Adf 'LinkedService' $ls 'RUNS_ON' 'IntegrationRuntime' $LsToIr[$ls]
      }
    }

    if ($a.PSObject.Properties.Name -contains 'pipeline') {
      $called = Get-SafeProp $a.pipeline 'referenceName'
      Add-Edge $Edges $Sub $Rg $Adf 'Pipeline' $PipelineName 'CALLS' 'Pipeline' $called ("FromActivity=$actName")
    }

    if ($a.PSObject.Properties.Name -contains 'dataflow') {
      $df = Get-SafeProp $a.dataflow 'referenceName'
      Add-Edge $Edges $Sub $Rg $Adf 'Pipeline' $PipelineName 'EXECUTES' 'DataFlow' $df ("FromActivity=$actName")
    }

    $children = Get-ActivityChildren -A $a
    if (@($children).Count -gt 0) {
      Walk-Activities -Edges $Edges -Inv $Inv -Sub $Sub -Rg $Rg -Adf $Adf -PipelineName $PipelineName -Activities $children -DsToLs $DsToLs -LsToIr $LsToIr -IncludeActivityInventory:$IncludeActivityInventory
    }
  }
}

Assert-Cli
& az login --only-show-errors | Out-Null

$factories = Import-Csv $InputCsv
$inv   = New-Object 'System.Collections.Generic.List[object]'
$edges = New-Object 'System.Collections.Generic.List[object]'
$errs  = New-Object 'System.Collections.Generic.List[string]'
$factoryIndex = 0

foreach ($f in $factories) {
  $factoryIndex++
  $sub = $f.SubscriptionId
  $rg  = $f.ResourceGroupName
  $adf = $f.DataFactoryName

  # Throttle between factories to avoid ARM rate limiting (HTTP 429)
  if ($factoryIndex -gt 1 -and $ThrottleDelayMs -gt 0) {
    Write-Host ("  Throttle pause {0}ms before factory {1}/{2}..." -f $ThrottleDelayMs, $factoryIndex, $factories.Count) -ForegroundColor DarkGray
    Start-Sleep -Milliseconds $ThrottleDelayMs
  }

  try {
    & az account set -s $sub --only-show-errors | Out-Null

    $baseUrl = "https://management.azure.com/subscriptions/$sub/resourceGroups/$rg/providers/Microsoft.DataFactory/factories/$adf"
    $apiVer  = 'api-version=2018-06-01'

    $factory = AzJson @('rest','--method','get','--url',"$baseUrl`?$apiVer")
    Add-InvRow $inv $sub $rg $adf 'DataFactory' $adf 'Factory' '' $(if ($IncludePropertiesJson) { To-CompactJson $factory } else { $null })

    $gpUrl  = "$baseUrl/globalParameters?$apiVer"
    foreach ($gpRes in (AzJsonPaged $gpUrl)) {
      $props = Get-SafeProp $gpRes 'properties'
      if ($props) {
        foreach ($p in $props.PSObject.Properties) {
          $gpType = Get-SafeProp $p.Value 'type'
          Add-InvRow $inv $sub $rg $adf 'GlobalParameter' $p.Name $gpType '' $(if ($IncludePropertiesJson) { To-CompactJson $p.Value } else { $null })
        }
      }
    }

    $credUrl  = "$baseUrl/credentials?$apiVer"
    foreach ($c in (AzJsonPaged $credUrl)) {
      Add-InvRow $inv $sub $rg $adf 'Credential' $c.name (Get-SafeProp $c.properties 'type') '' $(if ($IncludePropertiesJson) { To-CompactJson $c.properties } else { $null })
    }

    foreach ($ir in (AzJsonPaged "$baseUrl/integrationRuntimes?$apiVer")) {
      Add-InvRow $inv $sub $rg $adf 'IntegrationRuntime' $ir.name (Get-SafeProp $ir.properties 'type') '' $(if ($IncludePropertiesJson) { To-CompactJson $ir.properties } else { $null })
    }

    $LsToIr = @{}
    foreach ($ls in (AzJsonPaged "$baseUrl/linkedservices?$apiVer")) {
      $irName = $null
      if ($ls.properties.PSObject.Properties.Name -contains 'connectVia') {
        $irName = Get-SafeProp $ls.properties.connectVia 'referenceName'
      }
      if (-not $irName) { $irName = 'AutoResolveIntegrationRuntime' }
      $LsToIr[$ls.name] = $irName

      Add-InvRow $inv $sub $rg $adf 'LinkedService' $ls.name (Get-SafeProp $ls.properties 'type') "IR=$irName" $(if ($IncludePropertiesJson) { To-CompactJson $ls.properties } else { $null })
      Add-Edge  $edges $sub $rg $adf 'LinkedService' $ls.name 'RUNS_ON' 'IntegrationRuntime' $irName
    }

    $DsToLs = @{}
    foreach ($ds in (AzJsonPaged "$baseUrl/datasets?$apiVer")) {
      $lsRef = $null
      if ($ds.properties.PSObject.Properties.Name -contains 'linkedServiceName') {
        $lsRef = Get-SafeProp $ds.properties.linkedServiceName 'referenceName'
      }
      $DsToLs[$ds.name] = $lsRef

      Add-InvRow $inv $sub $rg $adf 'Dataset' $ds.name (Get-SafeProp $ds.properties 'type') "LinkedService=$lsRef" $(if ($IncludePropertiesJson) { To-CompactJson $ds.properties } else { $null })
      Add-Edge  $edges $sub $rg $adf 'Dataset' $ds.name 'USES' 'LinkedService' $lsRef
    }

    foreach ($df in (AzJsonPaged "$baseUrl/dataflows?$apiVer")) {
      Add-InvRow $inv $sub $rg $adf 'DataFlow' $df.name (Get-SafeProp $df.properties 'type') '' $(if ($IncludePropertiesJson) { To-CompactJson $df.properties } else { $null })
    }

    foreach ($t in (AzJsonPaged "$baseUrl/triggers?$apiVer")) {
      Add-InvRow $inv $sub $rg $adf 'Trigger' $t.name (Get-SafeProp $t.properties 'type') '' $(if ($IncludePropertiesJson) { To-CompactJson $t.properties } else { $null })
    }

    foreach ($mvn in (AzJsonPaged "$baseUrl/managedVirtualNetworks?$apiVer")) {
      Add-InvRow $inv $sub $rg $adf 'ManagedVirtualNetwork' $mvn.name 'ManagedVirtualNetwork' '' $(if ($IncludePropertiesJson) { To-CompactJson $mvn.properties } else { $null })

      foreach ($mpe in (AzJsonPaged "$baseUrl/managedVirtualNetworks/$($mvn.name)/managedPrivateEndpoints?$apiVer")) {
        $gid   = Get-SafeProp $mpe.properties 'groupId'
        $connState = Get-SafeProp $mpe.properties 'connectionState'
        $state = Get-SafeProp $connState 'status'
        $plrid = Get-SafeProp $mpe.properties 'privateLinkResourceId'

        Add-InvRow $inv $sub $rg $adf 'ManagedPrivateEndpoint' $mpe.name 'ManagedPrivateEndpoint' "GroupId=$gid;State=$state" $(if ($IncludePropertiesJson) { To-CompactJson $mpe.properties } else { $null })
        Add-Edge  $edges $sub $rg $adf 'ManagedPrivateEndpoint' $mpe.name 'CONNECTS_TO' 'AzureResource' $plrid ("GroupId=$gid;State=$state")
      }
    }

    foreach ($p in (AzJsonPaged "$baseUrl/pipelines?$apiVer")) {
      Add-InvRow $inv $sub $rg $adf 'Pipeline' $p.name 'Pipeline' '' $(if ($IncludePropertiesJson) { To-CompactJson $p.properties } else { $null })

      $pFull = AzJson @('rest','--method','get','--url',"$baseUrl/pipelines/$($p.name)?$apiVer")
      $pActivities = @()
      if ($pFull -and $pFull.PSObject.Properties.Name -contains 'properties') {
        if ($pFull.properties.PSObject.Properties.Name -contains 'activities') {
          $pActivities = @($pFull.properties.activities)
        }
      }
      Walk-Activities -Edges $edges -Inv $inv -Sub $sub -Rg $rg -Adf $adf -PipelineName $p.name -Activities $pActivities -DsToLs $DsToLs -LsToIr $LsToIr -IncludeActivityInventory
    }

    Write-Host ("OK: {0}" -f $adf) -ForegroundColor Green
  }
  catch {
    $msg = ('ERROR: {0} ({1} / {2}) :: {3}' -f $adf, $rg, $sub, $_.Exception.Message)
    $errs.Add($msg) | Out-Null
    Write-Host $msg -ForegroundColor Red
  }
}

$inv   | Export-Csv -Path $InventoryCsv -NoTypeInformation -Encoding UTF8
$edges | Export-Csv -Path $LineageCsv  -NoTypeInformation -Encoding UTF8

if ($errs.Count -gt 0) {
  $errs | Out-File -FilePath $ErrorLog -Encoding UTF8
  Write-Host ("Completed with errors. See {0}" -f $ErrorLog) -ForegroundColor Yellow
}

Write-Host ("Inventory CSV: {0}" -f $InventoryCsv) -ForegroundColor Cyan
Write-Host ("Lineage CSV:   {0}" -f $LineageCsv)  -ForegroundColor Cyan
