#!/usr/bin/env pwsh
$ScriptPath = $MyInvocation.MyCommand.Path
$ScriptDir = Split-Path -Parent $ScriptPath
$DockerContext = Resolve-Path "$ScriptDir\..\..\.."
docker build -t synapseml-host -f "$ScriptDir\Dockerfile" $DockerContext
