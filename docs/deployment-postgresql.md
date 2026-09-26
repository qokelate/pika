# PostgreSQL 版本部署指南

## 环境要求

- Docker 20.10+
- Docker Compose 1.29+

## 快速开始

### 1. 下载配置文件

```bash
# 下载 docker-compose.yml 配置文件
curl -O https://raw.githubusercontent.com/dushixiang/pika/main/docker-compose.postgresql.yml
# 下载配置文件示例
curl -o config.yaml https://raw.githubusercontent.com/dushixiang/pika/main/config.postgresql.yaml

# 或使用 wget
wget https://raw.githubusercontent.com/dushixiang/pika/main/docker-compose.postgresql.yml
wget -O config.yaml https://raw.githubusercontent.com/dushixiang/pika/main/config.postgresql.yaml
```

### 2. 修改配置

数据库配置（PostgreSQL）：

```yaml
database:
  enabled: true
  type: postgres
  postgres:
    hostname: pika-postgresql  # Docker Compose 服务名
    port: 5432
    username: pika
    password: pika  # 生产环境建议修改
    database: pika
```

**注意**：如果修改了 `docker-compose.postgresql.yml` 中的数据库密码（`POSTGRES_PASSWORD`），也需要同步修改 `config.yaml` 中的数据库密码。

服务端使用共享数据库连接池处理所有探针请求，PostgreSQL 默认最多保持 64 个打开连接、16 个空闲连接；连接空闲 5 分钟或存活 30 分钟后会自动回收，不会按探针数量创建一一对应的数据库连接。

其他配置项请参考 [通用配置说明](common-config.md)。

### 3. 启动服务

```bash
# 启动所有服务
docker-compose -f docker-compose.postgresql.yml up -d

# 查看服务状态
docker-compose -f docker-compose.postgresql.yml ps

# 查看日志
docker-compose -f docker-compose.postgresql.yml logs -f pika
```

### 4. 访问服务

服务启动后，访问 http://localhost:8080

默认账户：
- 用户名: `admin`
- 密码: `admin123`

### 5. 停止服务

```bash
# 停止服务
docker-compose -f docker-compose.postgresql.yml stop

# 停止并删除容器
docker-compose -f docker-compose.postgresql.yml down

# 停止并删除容器及数据卷
docker-compose -f docker-compose.postgresql.yml down -v
```

## 生产环境安全配置

- 修改 `docker-compose.postgresql.yml` 中的默认数据库密码（`POSTGRES_PASSWORD`）
- 修改 `config.yaml` 中的数据库密码（`database.postgres.password`），与 docker-compose 中的设置保持一致
- 限制数据库端口仅允许内部访问（docker-compose.yml 中已配置为 `127.0.0.1:5432:5432`）

其他安全配置请参考 [通用配置说明](common-config.md)。

## 数据持久化

系统数据分别存储在以下目录：
- **PostgreSQL 数据**：`./data/postgresql` - 存储配置和审计数据
- **VictoriaMetrics 数据**：`./data/vmdata` - 存储时序指标数据（默认保留 7 天）

### 备份 PostgreSQL 数据库

```bash
# 备份数据库
docker-compose -f docker-compose.postgresql.yml exec postgresql pg_dump -U pika pika > backup.sql

# 恢复数据库
docker-compose -f docker-compose.postgresql.yml exec -T postgresql psql -U pika pika < backup.sql
```

### 备份 VictoriaMetrics 数据

```bash
# 备份 VictoriaMetrics 数据
tar -czf vmdata-backup-$(date +%Y%m%d).tar.gz ./data/vmdata

# 恢复时停止服务，解压到 ./data/vmdata 目录即可
```

## 故障排查

### 数据库连接失败

- 确认 PostgreSQL 容器已启动且健康检查通过
- 检查 `config.yaml` 中的数据库配置是否正确：
  - `hostname` 应该为 `pika-postgresql`（Docker Compose 服务名）
  - `password` 应该与 `docker-compose.postgresql.yml` 中的 `POSTGRES_PASSWORD` 一致
- 确认 `config.yaml` 文件已正确映射到容器中
- 查看数据库日志：`docker-compose -f docker-compose.postgresql.yml logs postgresql`
- 查看应用日志：`docker-compose -f docker-compose.postgresql.yml logs pika`

其他故障排查请参考 [通用配置说明](common-config.md)。
