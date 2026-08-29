# demo

A minimal, runnable hazel host and plugin.

## Build

Build the plugin binary into `plugins/demo/`, matching the `cmdName` in its
`plugin.yaml`:

```sh
go build -o plugins/demo/demo ./plugin
```

> On Windows, `go build -o` appends `.exe`, producing `plugins/demo/demo.exe`;
> set `cmdName: demo.exe` in `plugins/demo/plugin.yaml` in that case.

## Run

Run the host from this directory (`PluginDirs: ["plugins"]` is relative to it):

```sh
go run ./host
```

The host discovers `plugins/demo/plugin.yaml`, loads and starts the plugin,
delivers its config and environment, publishes a `demo.ping` event, then pushes
a `config.changed` update. The plugin's logs are merged into the host's stderr.
