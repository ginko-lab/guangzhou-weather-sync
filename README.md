# 广州天气 & 空气质量 云端定时同步

每天北京时间 08:00 由 GitHub Actions 自动运行 `sync_turso.py`，从 open-meteo 拉取
广州（23.09N / 113.25E）天气与空气质量数据，计算中国 AQI 后写入 Turso 云数据库。
完全脱离本地电脑，无需开机。

## 结构

- `sync_turso.py` —— 数据同步脚本（token 从环境变量读取，不硬编码）
- `requirements.txt` —— 依赖（仅 `libsql`）
- `.github/workflows/sync.yml` —— 定时工作流（cron `0 0 * * *`，UTC；即北京时间 08:00）

## 密钥

Turso 数据库写权限 token 存放在仓库 Settings → Secrets and variables → Actions 的
`TURSO_TOKEN` 里，脚本运行时通过环境变量注入，不会出现在代码或日志中。

## 手动触发

在仓库 **Actions** 页选中「Sync Guangzhou Weather & Air to Turso」工作流，
点击 **Run workflow** 即可立即运行一次。