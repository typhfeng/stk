#!/usr/bin/env bash
# =============================================================================
# Mount Script: /dev/sde -> /mnt/dev/sde/A_stock/L2
# =============================================================================
# 用途: 将 /dev/sde 挂载到 /mnt/dev/sde/A_stock/L2
# 说明: 使用 XFS 优化的挂载参数，适用于 A股L2数据存储
# =============================================================================

set -euo pipefail

# 配置
# 注意: 根据实际情况，设备可能是 /dev/sda1 或 /dev/sde1
# 脚本会自动检测可用的设备
DEVICE="/dev/sda"  # 优先尝试 /dev/sda，如果不存在则尝试 /dev/sde
MOUNTPOINT="/mnt/dev/sde/A_stock/L2"
LOG_DIR=".local_changes/mount"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# 切换到仓库根目录
cd "$REPO_ROOT"

# 创建日志目录
mkdir -p "$LOG_DIR"

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_DIR/mount.log"
}

log "开始挂载脚本执行"

# 智能设备检测：优先尝试 /dev/sda1，然后 /dev/sde1
PARTITION=""
for dev in "/dev/sda1" "/dev/sde1" "/dev/sda" "/dev/sde"; do
    if [ -b "$dev" ]; then
        # 检查是否是分区（有数字后缀）
        if [[ "$dev" =~ [0-9]$ ]]; then
            PARTITION="$dev"
            log "找到分区设备: $PARTITION"
            break
        elif [ -b "${dev}1" ]; then
            PARTITION="${dev}1"
            log "找到分区设备: $PARTITION"
            break
        else
            PARTITION="$dev"
            log "找到设备: $PARTITION (整盘，无分区)"
            break
        fi
    fi
done

if [ -z "$PARTITION" ]; then
    log "错误: 未找到可用的设备 (/dev/sda1, /dev/sde1, /dev/sda, /dev/sde)"
    echo "可用设备列表:" | tee -a "$LOG_DIR/mount.log"
    lsblk -o NAME,MODEL,SIZE,FSTYPE,MOUNTPOINT | tee -a "$LOG_DIR/mount.log"
    exit 1
fi

# 创建挂载点
log "创建挂载点: $MOUNTPOINT"
sudo mkdir -p "$MOUNTPOINT" 2>&1 | tee -a "$LOG_DIR/mount.log"

# 检查是否已挂载
if mountpoint -q "$MOUNTPOINT"; then
    log "挂载点 $MOUNTPOINT 已经挂载"
    mount | grep "$MOUNTPOINT" | tee -a "$LOG_DIR/mount_check.txt"
    exit 0
fi

# 获取文件系统类型
FSTYPE=$(blkid -s TYPE -o value "$PARTITION" 2>/dev/null || echo "xfs")
log "检测到文件系统类型: $FSTYPE"

# 挂载参数（XFS优化参数，参考 doc/FileSystem.md）
if [ "$FSTYPE" = "xfs" ]; then
    MOUNT_OPTS="noatime,nodiratime,attr2,inode64,logbufs=8,logbsize=32k"
    log "使用 XFS 优化挂载参数: $MOUNT_OPTS"
else
    MOUNT_OPTS="noatime,nodiratime"
    log "使用默认挂载参数: $MOUNT_OPTS"
fi

# 执行挂载
log "挂载 $PARTITION 到 $MOUNTPOINT"
if sudo mount -o "$MOUNT_OPTS" "$PARTITION" "$MOUNTPOINT" 2>&1 | tee -a "$LOG_DIR/mount_output.txt"; then
    log "挂载成功"
else
    log "错误: 挂载失败"
    exit 1
fi

# 设置目录归属为当前用户（方便后续操作）
log "设置目录归属"
sudo chown -R "$(id -u):$(id -g)" "$MOUNTPOINT" 2>&1 | tee -a "$LOG_DIR/mount.log" || true

# 验证挂载状态
log "验证挂载状态"
mount | grep "$MOUNTPOINT" | tee -a "$LOG_DIR/mount_check.txt"
df -h "$MOUNTPOINT" | tee -a "$LOG_DIR/mount_check.txt"

# 列出挂载点内容（前50行）
log "列出挂载点内容"
ls -la "$MOUNTPOINT" 2>&1 | head -50 | tee -a "$LOG_DIR/ls_top.txt" || true

# 如果是XFS，显示文件系统信息
if [ "$FSTYPE" = "xfs" ]; then
    log "XFS 文件系统信息"
    xfs_info "$MOUNTPOINT" 2>&1 | head -25 | tee -a "$LOG_DIR/xfs_info.txt" || true
fi

log "挂载脚本执行完成"
