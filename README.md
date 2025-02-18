# SMB 文件传输系统

## 项目概述
本系统是基于SMB协议实现的自动化文件传输解决方案，用于从多台Windows主机自动同步指定目录文件到本地服务器。支持哈希校验、动态负载均衡、自动错误恢复等功能，适用于企业级文件同步场景。

## 功能特性
- 🚀 多线程并行传输（支持动态线程调整）
- 🔒 传输完成后本地哈希校验
- 📊 每日邮件发送传输统计报告
- ⚡ 配置文件热更新（无需重启服务）
- 📁 智能日志管理（自动压缩/归档）
- 🛡️ 资源使用监控与自动优化
- 🔄 断点续传与错误重试机制

## 系统要求
- Windows 10/Server 2016+
- Python 3.10+
- 网络开放TCP 445端口(SMB)

## 快速开始

### 安装步骤
```powershell
# 1. 克隆仓库
git clone https://example.com/smb-transfer-system.git

# 2. 安装依赖
pip install -r requirements.txt

# 3. 设置计划任务（管理员权限运行）
$Trigger = New-ScheduledTaskTrigger -Daily -At 8am
$Action = New-ScheduledTaskAction -Execute "python" -Argument "$PWD\transfer_system.py"
Register-ScheduledTask -TaskName "FileTransfer" -Trigger $Trigger -Action $Action -RunLevel Highest
