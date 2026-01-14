# 每日挂载任务 — /dev/sda1 -> /mnt/dev/sde/A_stock/L2

目的
- 每天确保本机的 `/dev/sda1` 已挂载到 `/mnt/dev/sde/A_stock/L2`，以便仓库的日常任务和数据访问可用。

行为约定
- Agent 在执行前会把操作输出保存到仓库的 `.local_changes/mount/`（例如 `mount_output.txt`、`mount_check.txt`），并采用最小权限的步骤：创建挂载点、尝试挂载、设置属主、并做可选的本地验证。操作需要 `sudo` 权限。

一次性运行示例（agent 可直接执行）
```bash
# 创建挂载点（如果不存在）
sudo mkdir -p /mnt/dev/sde/A_stock/L2

# 如果未挂载则挂载（使用 mountpoint 检测），并记录输出到 .local_changes
mkdir -p .local_changes/mount || true
if ! mountpoint -q /mnt/dev/sde/A_stock/L2; then
  sudo mount /dev/sda1 /mnt/dev/sde/A_stock/L2 2>&1 | tee .local_changes/mount/mount_output.txt
else
  echo "already mounted" | tee .local_changes/mount/mount_output.txt
fi

# 设置目录归属为当前用户，方便后续操作
sudo chown $(id -u):$(id -g) /mnt/dev/sde/A_stock/L2 2>/dev/null || true

# 验证并记录状态
mount | grep /mnt/dev/sde/A_stock/L2 2>&1 | tee .local_changes/mount/mount_check.txt
ls -la /mnt/dev/sde/A_stock/L2 | sed -n '1,50p' > .local_changes/mount/ls_top.txt || true
```

每日调度（推荐）
- 推荐使用 `systemd` timer（比 cron 更可控，日志集中）。示例 unit 与 timer：

`/etc/systemd/system/stk-mount-sda1.service`:
```ini
[Unit]
Description=Ensure /dev/sda1 is mounted at /mnt/dev/sde/A_stock/L2

[Service]
Type=oneshot
ExecStart=/usr/bin/env bash -c 'mkdir -p /mnt/dev/sde/A_stock/L2 && mountpoint -q /mnt/dev/sde/A_stock/L2 || mount /dev/sda1 /mnt/dev/sde/A_stock/L2'
```

`/etc/systemd/system/stk-mount-sda1.timer`:
```ini
[Unit]
Description=Daily timer to ensure /dev/sda1 is mounted

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

启用并立即运行一次：
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now stk-mount-sda1.timer
sudo systemctl start stk-mount-sda1.service   # 立即运行一次
```

Cron 备选（简单）
- 若不使用 systemd，可在 root 的 crontab 加入：
```cron
@daily /bin/bash -c 'mountpoint -q /mnt/dev/sde/A_stock/L2 || mount /dev/sda1 /mnt/dev/sde/A_stock/L2'
```

安全与注意事项
- 挂载操作需要 `sudo`，agent 会把命令输出保存到 `.local_changes/mount/` 便于审计与回滚。
- 推荐使用 `UUID=` 或 device path 在 fstab 中配置长期挂载；使用 systemd timer 可以减少对 fstab 的侵入。
- 如果设备名会变化（例如经常插拔），请改用 `UUID`（`blkid /dev/sda1` 查看），或在脚本中加入更健壮的设备检测逻辑。

下一步（可选）
- 我可以现在为你在本机启用 `systemd` 定时器并立即运行一次（需要你确认）。
- 我也可以把这份文件的要点合并回 `.github/agents/stk-research.agent.md` 主文档（需要确认）。
