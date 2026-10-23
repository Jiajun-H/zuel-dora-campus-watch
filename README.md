# 哆啦校圈监控

> 面向中南财经政法大学哆啦校圈的本地悬赏监控工具：定时检查、严格核验、SQLite 去重，并通过 Bark 提醒手机。

[快速开始](#快速开始) · [终端命令](#终端命令) · [监控策略与边界](#监控策略与边界) · [数据与配置](#数据与配置) · [Windows 使用指南](docs/使用指南.md)

## 功能

- 每 60 秒检查最近的悬赏帖子，只提醒符合学校、分类、帖子 ID 和正文校验要求的新发现。
- 将帖子 ID 和通知状态保存在 SQLite，重启后继续去重。
- 通过独立后台进程运行；TUI 关闭不会停止监控。
- Bark 推送结果不明时保留待核对状态，避免盲目重发。
- 使用有界列表请求预算；启动和长时间离线恢复时静默同步，不批量补发历史帖。

## 快速开始

### 离线体验

需要 Python 3.12 和 [uv](https://docs.astral.sh/uv/)。在项目根目录运行：

~~~powershell
uv sync --locked
uv run --locked python -m campus_watch demo
~~~

演示使用合成帖子和临时数据库，不连接真实论坛，也不发送 Bark。

### Windows 实时监控

1. 克隆仓库并安装锁定依赖：

   ~~~powershell
   git clone https://github.com/Jiajun-H/zuel-dora-campus-watch.git
   cd zuel-dora-campus-watch
   uv sync --locked
   ~~~

2. 准备本机正文解密配置 `data/body_cipher.local.json`。它不会随仓库分发，也不应提交到 Git。
3. 双击项目根目录的 `校圈监控.cmd`；也可以在 PowerShell 中运行：

   ~~~powershell
   & .\校圈监控.cmd
   ~~~

4. 在界面底部输入 `/start`。如果本机还没有 Bark Key，启动流程会以隐藏输入方式询问；若 freshKey 未提供，监控会按现有策略自动获取。

打开界面不会自动启动监控。自建 Bark 服务可通过 Windows 环境变量 `BARK_SERVER` 配置；未设置时使用 `https://api.day.app`。更多 Windows 配置和旧版启停入口见 [使用指南](docs/使用指南.md)。

## 终端命令

| 命令 | 作用 |
| --- | --- |
| `/start` | 启动监控；若后台已运行，则连接现有实例 |
| `/status` | 查看状态、统计、freshKey 和运行诊断 |
| `/posts` | 浏览最近 100 条历史帖子 |
| `/home` | 返回实时帖子消息流 |
| `/help` | 查看命令帮助 |
| `/stop` | 二次确认后请求后台受控停止 |
| `/quit` | 只关闭界面，后台继续运行 |

帖子消息流只在发现新帖子或帖子通知状态变化时更新。Bark 已接受表示 Bark 服务端接受了请求，不代表手机实际送达。

## 监控策略与边界

- 默认每轮读取最近 2 页，最多使用 3 次列表请求；旧锚点和页级证据用于诊断，不作为正常扫描的暂停门槛。
- 每页数据会验证学校、悬赏分类、帖子 ID 和正文。必要请求失败时，不提交不完整的一轮观察。
- 首次启动和观测间隔超过 15 分钟时静默同步当前窗口，避免离线期间的历史帖子被批量补发。
- 已成功通知的帖子 ID 持久化防重；帖子置顶或重排不会因此重复通知。
- 固定页数存在边界：帖子可能在两次检查之间移出扫描窗口而漏掉；此前从未入库的旧帖后来进入窗口时，可能被当作首次发现通知。

## 数据与配置

数据库、运行状态、日志、Bark 凭证和正文解密参数都保存在本机 `data/` 目录，该目录已从 Git 提交中排除。Bark Key 使用当前 Windows 用户的本机保护机制保存。不要把 `data/`、设备 Key 或运行数据库上传到公开仓库。

监控只适用于你有权访问的内容。请遵守来源平台规则，并根据本地状态确认运行和通知结果。

## 开发与离线验证

~~~powershell
uv sync --locked
uv run --locked python -m campus_watch demo
uv run --locked python -m unittest tui_tests.test_tui
~~~

TUI 测试使用合成状态和临时 SQLite，不访问论坛、不发送 Bark。Windows 用户也可以从 [Windows 使用指南](docs/使用指南.md)查看日常运行方式。
