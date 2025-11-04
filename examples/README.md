# Keyper Observability Examples

This directory contains configuration files and examples for monitoring and observing Keyper clusters.

## Files

### 1. `prometheus-config.yml`
Prometheus configuration for scraping metrics from a Keyper cluster.

**Usage**:
```bash
prometheus --config.file=examples/prometheus-config.yml
```

**Features**:
- Scrapes 3-node cluster on ports 8080, 8081, 8082
- 15-second scrape interval
- Includes alertmanager integration
- Supports remote write for long-term storage

**Customization**:
- Edit `targets` to match your cluster endpoints
- Adjust `scrape_interval` based on your needs
- Configure `remote_write` for Grafana Cloud or other backends

---

### 2. `keyper-alerts.yml`
Prometheus alerting rules for Keyper.

**Usage**:
Place in the same directory as `prometheus-config.yml` and reference in the `rule_files` section.

**Alert Groups**:
- **keyper_raft**: Raft consensus alerts (no leader, high latency, etc.)
- **keyper_http**: HTTP API alerts (high error rate, high latency)
- **keyper_storage**: BadgerDB storage alerts (growth, failures)
- **keyper_shards**: Shard alerts (read-only, migration failures)
- **keyper_control_plane**: Control plane alerts (node loss)
- **keyper_health**: Health check alerts
- **keyper_recording_rules**: Pre-aggregated metrics for dashboards

**Alert Severity Levels**:
- `critical`: Requires immediate action (no leader, migration failed)
- `warning`: Requires attention (high latency, read-only shard)
- `info`: Informational (node restart)

**Runbooks**:
Update the `runbook_url` annotations with your actual documentation URLs.

---

### 3. `grafana-dashboard.json`
Grafana dashboard template for visualizing Keyper metrics.

**Import**:
1. Open Grafana UI
2. Click "+" → "Import"
3. Paste the contents of `grafana-dashboard.json`
4. Select your Prometheus datasource
5. Click "Import"

**Dashboard Panels**:
1. **Cluster Overview** - Node and shard counts
2. **Raft Leaders** - Number of Raft leaders
3. **HTTP Request Rate** - Requests/sec by status code
4. **HTTP Latency (P99)** - Request latency by path
5. **Raft Commit Latency** - P50/P95/P99 commit times
6. **Raft Operations** - Operations/sec by type (set/delete)
7. **BadgerDB Write Throughput** - Bytes/sec per shard
8. **BadgerDB Storage Size** - LSM and VLog sizes
9. **Shard Operations** - Operations/sec by shard and type
10. **Shard Migrations** - Migration rate by status
11. **Raft State by Shard** - Leader/Follower/Candidate status
12. **Shard Read-Only Status** - Which shards are read-only
13. **Node Uptime** - Time since node start

**Auto-Refresh**: Dashboard refreshes every 10 seconds by default.

---

## Quick Start

### 1. Start Keyper Cluster
```bash
# Terminal 1: Node 1
./bin/server -node-id=node1 -http-addr=:8080 -raft-addr=localhost:9080

# Terminal 2: Node 2
./bin/server -node-id=node2 -http-addr=:8081 -raft-addr=localhost:9081

# Terminal 3: Node 3
./bin/server -node-id=node3 -http-addr=:8082 -raft-addr=localhost:9082
```

### 2. Start Prometheus
```bash
cd /path/to/keyper
prometheus --config.file=examples/prometheus-config.yml
```

Access Prometheus UI at: http://localhost:9090

### 3. Verify Metrics
```bash
# Check /metrics endpoint
curl http://localhost:8080/metrics | grep keyper_

# Query Prometheus
curl 'http://localhost:9090/api/v1/query?query=keyper_raft_state'
```

### 4. Import Grafana Dashboard
1. Install Grafana (if not already installed):
   ```bash
   # macOS
   brew install grafana
   brew services start grafana
   
   # Ubuntu
   sudo apt-get install -y grafana
   sudo systemctl start grafana-server
   ```

2. Access Grafana UI: http://localhost:3000 (default: admin/admin)

3. Add Prometheus datasource:
   - Configuration → Data Sources → Add data source
   - Select "Prometheus"
   - URL: `http://localhost:9090`
   - Click "Save & Test"

4. Import dashboard:
   - Create → Import
   - Paste contents of `examples/grafana-dashboard.json`
   - Select Prometheus datasource
   - Click "Import"

### 5. Test Alerts
Trigger alerts by causing failures:

```bash
# Stop a node to trigger "NoRaftLeader"
pkill -f "server -node-id=node1"

# Generate high error rate
for i in {1..100}; do curl -X DELETE http://localhost:8080/v1/keys/nonexistent; done

# Create artificial latency (requires tc/netem)
# sudo tc qdisc add dev lo root netem delay 1000ms
```

Check alerts in Prometheus UI: http://localhost:9090/alerts

---

## Advanced Usage

### Prometheus Federation
For multi-cluster monitoring, use Prometheus federation:

```yaml
# Global Prometheus config
scrape_configs:
  - job_name: 'federate'
    scrape_interval: 15s
    honor_labels: true
    metrics_path: '/federate'
    params:
      'match[]':
        - '{job="keyper-cluster"}'
    static_configs:
      - targets:
        - 'prom-cluster-1:9090'
        - 'prom-cluster-2:9090'
```

### Remote Write to Grafana Cloud
```yaml
# In prometheus-config.yml
remote_write:
  - url: 'https://prometheus-us-central1.grafana.net/api/prom/push'
    basic_auth:
      username: '<your-instance-id>'
      password: '<your-api-key>'
```

### Custom Recording Rules
Add to `keyper-alerts.yml`:

```yaml
- name: custom_metrics
  interval: 1m
  rules:
    # Average shard size
    - record: keyper:shard:avg_size_bytes
      expr: avg(keyper_badger_lsm_size_bytes + keyper_badger_vlog_size_bytes) by (shard_id)
    
    # Total cluster throughput
    - record: keyper:cluster:total_ops_per_sec
      expr: sum(rate(keyper_raft_applied_ops_total[1m]))
```

### Health Check with Blackbox Exporter
Monitor `/v1/health` endpoints:

```yaml
# blackbox.yml
modules:
  http_keyper_health:
    prober: http
    http:
      valid_status_codes: [200]
      method: GET
      fail_if_not_matches_regexp:
        - '"ok":true'

# prometheus-config.yml
scrape_configs:
  - job_name: 'blackbox-health'
    metrics_path: /probe
    params:
      module: [http_keyper_health]
    static_configs:
      - targets:
        - http://localhost:8080/v1/health
        - http://localhost:8081/v1/health
        - http://localhost:8082/v1/health
    relabel_configs:
      - source_labels: [__address__]
        target_label: __param_target
      - source_labels: [__param_target]
        target_label: instance
      - target_label: __address__
        replacement: localhost:9115  # Blackbox exporter address
```

---

## Metric Reference

See `FEATURE_G_OBSERVABILITY.md` for complete metric documentation.

**Key Metrics**:
- `keyper_raft_commit_duration_seconds` - Raft commit latency
- `keyper_http_requests_total` - HTTP request counter
- `keyper_badger_writes_total` - Storage write operations
- `keyper_shard_operations_total` - Shard operations by type
- `keyper_raft_state` - Current Raft state (0=follower, 1=candidate, 2=leader)

**Useful Queries**:
```promql
# Cluster-wide write throughput (ops/sec)
sum(rate(keyper_raft_applied_ops_total{op_type="set"}[1m]))

# HTTP success rate
sum(rate(keyper_http_requests_total{status=~"2.."}[5m])) / sum(rate(keyper_http_requests_total[5m]))

# Average shard size
avg(keyper_badger_lsm_size_bytes + keyper_badger_vlog_size_bytes) by (shard_id)

# Raft leader count (should be 1 per shard)
count(keyper_raft_state == 2) by (shard_id)
```

---

## Troubleshooting

### Metrics not appearing
1. Check `/metrics` endpoint: `curl http://localhost:8080/metrics`
2. Verify Prometheus is scraping: Check "Status → Targets" in Prometheus UI
3. Check Prometheus logs for scrape errors
4. Ensure no firewall blocking port 8080

### Alerts not firing
1. Verify rules are loaded: Check "Status → Rules" in Prometheus UI
2. Test expression in Prometheus "Graph" view
3. Check `for` duration - alerts require sustained condition
4. Verify Alertmanager is running (if configured)

### Dashboard shows "No data"
1. Verify Prometheus datasource is configured correctly
2. Check that metrics exist in Prometheus: "Explore" view
3. Verify time range in dashboard (top-right)
4. Refresh dashboard or wait for next scrape interval

### High cardinality warnings
- Don't use unbounded labels (keys, user IDs, UUIDs)
- Normalize HTTP paths in metrics (use `:key` placeholders)
- Limit number of shards if using shard_id label
- Consider sampling for high-volume metrics

---

## Production Recommendations

1. **Retention**: Configure Prometheus retention (default 15 days):
   ```bash
   prometheus --storage.tsdb.retention.time=30d
   ```

2. **Storage**: Use remote write for long-term storage (Grafana Cloud, Thanos, Cortex)

3. **High Availability**: Run multiple Prometheus servers with identical config

4. **Alerting**: Configure Alertmanager for notifications (email, Slack, PagerDuty)

5. **Dashboards**: Create team-specific dashboards (SRE, developers, business)

6. **SLOs**: Define Service Level Objectives based on metrics:
   - HTTP P99 latency < 100ms
   - Raft commit P99 < 500ms
   - HTTP success rate > 99.9%

7. **Recording Rules**: Pre-aggregate expensive queries for dashboard performance

---

For more information, see `FEATURE_G_OBSERVABILITY.md`.
