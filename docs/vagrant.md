# Using the MMLSpark Vagrant Image

## Install Vagrant and Dependencies

You will need to a few dependencies before we get started. These instructions are for using Vagrant on Windows OS.

1. Ensure [Hyper-V](https://docs.microsoft.com/en-us/virtualization/hyper-v-on-windows/) is enabled or install [Virtualbox](https://www.virtualbox.org/)
2. Install a X Server for Windows, [VcXsrv](https://sourceforge.net/projects/vcxsrv/) is a lightweight option.
3. Install the Vagrant version for your OS [here](https://www.vagrantup.com/downloads.html)

## Build the Vagrant Image

Start powershell as Administrator and go to the `mmlspark/tools/vagrant` directory and run

```
vagrant up
```

*Note: you may need to select a network switch, try the Default Switch option if possible*

## Connect to the Vagrant Image

First start the X-Window server (XLaunch if using VcXsrv).

From the same directory (with powershell as Administrator) run

```
$env:DISPLAY="localhost:0"
vagrant ssh -- -Y

# now you can start IntelliJ and interact with the GUI
> idea
```


## Stop the Vagrant Image

```
vagrant halt
```

## Further reading

This guide covers the bare minimum for running a Vagrant image. For more details see the [Vagrant Documentation](https://www.vagrantup.com/intro/index.html).
