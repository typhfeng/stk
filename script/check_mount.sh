#!/usr/bin/env bash
# =============================================================================
# Check Mount Status Script
# =============================================================================
# 检查 /mnt/dev/sde/A_stock/L2 的挂载状态
# =============================================================================

set -euo pipefail

MOUNTPOINT="/mnt/dev/sde/A_stock/L2"

echo "=========================================="
echo "挂载状态检查: $MOUNTPOINT"
echo "=========================================="
echo

# 检查挂载点是否存在
if [ ! -d "$MOUNTPOINT" ]; then
    echo "✗ 挂载点不存在: $MOUNTPOINT"
    exit 1
fi

echo "✓ 挂载点存在: $MOUNTPOINT"
echo

# 检查是否已挂载
if mountpoint -q "$MOUNTPOINT"; then
    echo "✓ 已挂载"
    echo
    
    # 显示挂载信息
    echo "挂载信息:"
    mount | grep "$MOUNTPOINT" | sed 's/^/  /'
    echo
    
    # 显示磁盘使用情况
    echo "磁盘使用情况:"
    df -h "$MOUNTPOINT" | sed 's/^/  /'
    echo
    
    # 显示文件系统信息（如果是XFS）
    FSTYPE=$(df -T "$MOUNTPOINT" | tail -1 | awk '{print $2}')
    if [ "$FSTYPE" = "xfs" ]; then
        echo "XFS 文件系统信息:"
        xfs_info "$MOUNTPOINT" 2>&1 | head -10 | sed 's/^/  /' || true
        echo
    fi
    
    # 列出目录内容（前10项）
    echo "目录内容（前10项）:"
    ls -lh "$MOUNTPOINT" 2>&1 | head -11 | sed 's/^/  /'
    echo
    
    echo "=========================================="
    echo "✓ 挂载状态正常"
    echo "=========================================="
    exit 0
else
    echo "✗ 未挂载"
    echo
    echo "可用设备列表:"
    lsblk -o NAME,MODEL,SIZE,FSTYPE,MOUNTPOINT | grep -E "sda|sde|NAME" | sed 's/^/  /'
    echo
    echo "要挂载，请运行:"
    echo "  sudo bash script/mount_sde.sh"
    echo "或手动挂载:"
    echo "  sudo mount -o noatime,nodiratime,attr2,inode64,logbufs=8,logbsize=32k /dev/sda1 $MOUNTPOINT"
    echo
    echo "=========================================="
    echo "✗ 挂载点存在但未挂载"
    echo "=========================================="
    exit 1
fi
