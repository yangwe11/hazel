# hazel

> [English](README.md)

Hazel 是一个基于 [hashicorp/go-plugin] 的 Go 插件框架。它把应用拆成一个**主进程**
加若干个**插件进程**，通过 `net/rpc` 通信，内置了生命周期管理、事件总线、配置下发、
扩展注册和日志收集。

[hashicorp/go-plugin]: https://github.com/hashicorp/go-plugin

## 特性

- **生命周期** — 发现、加载、初始化、启动、停止插件，支持依赖排序的并行启动、崩溃监控与自动恢复。
- **事件总线** — 主进程与插件按点分隔的主题模式发布/订阅（`plugin.running`、`config.changed` 等）。
- **配置** — 初始化时下发每插件的 JSON 配置，运行时通过 `config.changed` 事件热更新。
- **环境** — 向每个插件下发共享的主进程事实（引擎版本、数据目录、自定义属性）。
- **扩展** — 用两行注册即可在框架外新增主进程↔插件能力。
- **日志收集** — 插件 stderr/stdout 合并进主进程的 `hclog` 日志，带插件名前缀。

## 安装

```
go get github.com/yangwe11/hazel
```

## 快速开始

### 插件

每个插件实现 `hazel.Lifecycle` 并由 `hazel.Serve` 服务，二进制旁放一个 `plugin.yaml`。

```go
package main

import "github.com/yangwe11/hazel"

type Greeter struct{ ctx hazel.Context }

// SetContext 可选；实现它即可在 Initialize 时收到 Context。
func (g *Greeter) SetContext(ctx hazel.Context) { g.ctx = ctx }

func (g *Greeter) Initialize(hazel.InitializeArgs) error { return nil }
func (g *Greeter) Start(hazel.StartArgs) error            { return nil }
func (g *Greeter) Stop() error                            { return nil }

func main() {
    hazel.Serve(&Greeter{})
}
```

### 主进程

```go
package main

import "github.com/yangwe11/hazel"

func main() {
    m, err := hazel.NewManager(hazel.ManagerConfig{
        PluginDirs: []string{"./plugins"},
    })
    if err != nil {
        panic(err)
    }
    defer m.Shutdown()

    if _, err := m.Discover(); err != nil {
        panic(err)
    }
    for _, id := range []string{"greeter"} {
        m.Load(id)
        m.Initialize(id)
        m.Start(id)
    }
    // 插件已运行
}
```

## 核心概念

### 生命周期与状态

插件经历 `unloaded → loaded → initialized → running`，可终止于 `stopped` 或
`error`。主进程用 `Manager.Load`、`Manager.Initialize`、`Manager.Start`、
`Manager.Stop` 驱动。`StartArgs.First` 在插件于本次主进程会话中首次成功启动时为真。

### Context

实现 `hazel.ContextAware` 的插件会在 `Initialize` 时收到一个 `hazel.Context`，它是插件的核心句柄：

```go
ctx.ID()            // 插件 id
ctx.Host().Ping()   // 访问主进程
ctx.Bus()           // 事件总线
ctx.Config()        // 每插件配置，JSON 编码
ctx.Environment()   // 共享的主进程事实
```

### 事件总线

按点分隔的主题模式发布/订阅。`*` 匹配一段，`>` 匹配剩余段。

```go
ctx.Bus().Subscribe("config.changed", func(e hazel.Event) {
    // 处理事件
})
ctx.Bus().Publish(hazel.Event{Name: "app.ready", Payload: "started"})
```

### 配置

主进程为每个插件提供配置，插件自行解码：

```go
// 主进程
cfg := hazel.DefaultManagerConfig()
cfg.Config = func(id string) (any, error) {
    return map[string]any{"port": 8080}, nil
}

// 插件
var c struct{ Port int `json:"port"` }
json.Unmarshal(ctx.Config(), &c)
```

推送运行时更新——插件会收到 `config.changed` 事件：

```go
m.UpdateConfig("greeter", map[string]any{"port": 9090})
```

### 环境

主进程定义的共享事实通过 `Context.Environment()` 送达每个插件：

```go
env := ctx.Environment()
env.EngineVersion  // 运行中的引擎版本
env.DataDir        // ManagerConfig.DataDir
env.Attributes     // 主进程定义的 map[string]string
```

### 扩展

无需改动框架即可新增能力。在 `init()` 里注册，然后在两个二进制里 import 该包。

- **插件 → 主进程**：`hazel.RegisterHostService` — 主进程提供服务，插件通过 `*Aware`
  接口消费。`Server` 收到插件 ID，可选的 `Wire` 钩子在 Initialize 后运行，用于绑定
  host→plugin 方向。
- **主进程 → 插件**：`hazel.RegisterPluginService` — 插件提供服务，主进程通过
  `Manager.Dispense` 消费。
- **双向**：各注册一个；`Wire` 钩子自动接线 host→plugin 方向。见
  [`example/greeter`](example/greeter/greeter.go)。

一个真实的第一方扩展 [`health`](health) 就是这样实现的插件就绪/存活上报。

### 日志

日志统一走 `hclog`，由 `ManagerConfig.Logger` 配置（默认写 stderr、`Info` 级）。
主进程自身的生命周期消息和每个插件的日志都汇入它：go-plugin 逐行读取插件 stderr，
带插件名前缀重新输出。无级别前缀的 `log.Printf` 会被 go-plugin 归为 `Debug`。

### 发现

`Manager.Discover` 扫描 `PluginDirs` 下一级目录中的 `plugin.yaml`：

```yaml
id: greeter
name: Greeter
version: 1.0.0
type: NATIVE      # NATIVE | STATIC | BRIDGE
cmdName: greeter  # 可选，默认取 id
engineRequirement: ">=0.1.0"  # 可选
depends: []       # 可选，带版本约束
```

`STATIC` 插件仅由文件构成：会被发现并纳入生命周期管理，但不会作为进程启动（`Load` 只校验其目录）。

## License

[Apache 2.0](LICENSE) — Copyright (c) 2026 yangwe11。
