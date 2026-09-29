# Telemetry contract

The producer emits OTLP/HTTP protobuf using generated OTLP Go definitions v1.9.0.
It implements separate `/v1/metrics`, `/v1/logs` and `/v1/traces` requests to one
base endpoint. Transport responses are checked before source outbox deletion.

Semantic baseline: OpenTelemetry Semantic Conventions **v1.43.0**. Standard
resource attributes used here are stable `service.name` and
`deployment.environment.name`. Instrumentation scope identifies this adapter and
its version. Application resource name is `uipath-orchestrator`; adapter
self-metrics use `uipath-otel-adapter`. These are generic logical service names,
not discovered machine or deployment names. Environment and source identity are
explicit configuration. No host/container auto-detection can misattribute a
remote robot to the collector machine.

UiPath job/queue/folder semantics have no applicable standard convention in the
inspected model, so `uipath.*` is a custom contract. Process names describe UiPath
workflows, not operating-system processes. Job keys are log/span attributes only;
they never become metric dimensions.

| OTel metric | Kind / unit | Local Prometheus translation |
| --- | --- | --- |
| `uipath.folder.info` | Gauge / no unit, value 1; folder ID/name inventory | `uipath_folder_info` |
| `uipath.collector.dataset.enabled` | Gauge / `1`, by folder/dataset; 0 disabled, 1 enabled | `uipath_collector_dataset_enabled_ratio` |
| `uipath.jobs` | Gauge / `{job}` | `uipath_jobs` |
| `uipath.jobs.completed` | Monotonic cumulative Sum / `{job}` | `uipath_jobs_completed_total` |
| `uipath.job.duration` | Cumulative explicit Histogram / `s` | `uipath_job_duration_seconds_{bucket,sum,count}` |
| `uipath.job.queue_wait` | Cumulative explicit Histogram / `s` | `uipath_job_queue_wait_seconds_{bucket,sum,count}` |
| `uipath.job.last_duration` | Gauge / `s` | `uipath_job_last_duration_seconds` |
| `uipath.job.oldest_pending_age` | Gauge / `s` | `uipath_job_oldest_pending_age_seconds` |
| `uipath.job.oldest_running_age` | Gauge / `s` | `uipath_job_oldest_running_age_seconds` |
| `uipath.process.last_success_time` | Gauge, Unix timestamp / `s` | `uipath_process_last_success_time_seconds` |
| `uipath.queue.items` | Gauge / `{item}` | `uipath_queue_items` |
| `uipath.queue.oldest_pending_age` | Gauge / `s` | `uipath_queue_oldest_pending_age_seconds` |
| `uipath.collector.discovery.success` | Gauge / `1` | `uipath_collector_discovery_success_ratio` |
| `uipath.collector.scrape.success` | Gauge / `1`, by folder/dataset | `uipath_collector_scrape_success_ratio` |
| `uipath.collector.last_success_time` | Gauge, Unix timestamp / `s` | `uipath_collector_last_success_time_seconds` |
| `uipath.collector.outbox.pending` | Gauge / `{batch}` | `uipath_collector_outbox_pending` |
| `uipath.collector.outbox.quarantined` | Gauge / `{batch}` | `uipath_collector_outbox_quarantined` |

Histogram bounds (seconds): 1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 14400,
and overflow. OTLP bucket counts are per-bucket, not cumulative; the receiver
performs Prometheus conversion. Counter and histogram start timestamps survive
restart alongside their aggregates. Metrics use collection time; source event
history belongs in logs/traces. Bootstrap does not create current-rate spikes.

Metric dimensions are configured source identity, folder, process, state/outcome
and queue where applicable. A single polling authority per source partition is
required. The number of process groups grows with source inventory; operate with
an explicit folder allowlist and monitor resulting cardinality. The local
Collector preserves OTLP to Thanos, whose receiver explicitly promotes source
identity and environment to labels; production receivers should likewise use
an explicit promotion policy.

Robot logs use source TimeStamp and collection ObservedTime, native severity,
an optional message body and `uipath.*` identifiers. Job lifecycle events use job
end time. Messages are off by default; enabling them may expose source payload
content to the configured backend. Input/output arguments, queue payloads,
Windows identities and RawMessage are not fetched.

The local pipeline, not the adapter, requests Collector v0.148.0's ECS mapping.
Native OTLP context becomes ECS `trace.id` and `span.id`; source log time becomes
`@timestamp`; severity becomes `log.level`; string body becomes `message`.
These are explicit mappings of OTLP envelope fields, not OTel attribute aliases.
The adapter does not declare an ECS version or serialize ECS documents. The
Collector's ECS mapping is version-specific; this sample is not a full ECS
conformance claim. The local stream template fixes timestamp/message/ID types.

Sources:

- [OTLP protocol and partial success](https://opentelemetry.io/docs/specs/otlp/)
- [OTel service attributes, v1.43.0](https://github.com/open-telemetry/semantic-conventions/blob/v1.43.0/model/service/registry.yaml)
- [OTel deployment attributes, v1.43.0](https://github.com/open-telemetry/semantic-conventions/blob/v1.43.0/model/deployment/registry.yaml)
- [ECS tracing schema, v9.4.0](https://github.com/elastic/ecs/blob/v9.4.0/schemas/tracing.yml)
- [Collector Elasticsearch mapping, v0.148.0](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.148.0/exporter/elasticsearchexporter/README.md)
- [UiPath RobotLogs pagination restrictions](https://docs.uipath.com/orchestrator/standalone/2025.10/api-guide/robots-requests)

Folder inventory includes authorized discovered folders without jobs. Names are
joined to metrics in Grafana and attached to newly collected logs/spans as
`uipath.folder.name`; older events are not rewritten. Folder names are display
metadata, not stable identity. Full hierarchical paths are not yet collected.
Completed-job logs include numeric `uipath.job.duration` in seconds when valid
start/end timestamps are available. It describes elapsed time, including suspension.

## Queue snapshots and coverage

Queue collection reads `QueueDefinitions` (ID/name only) and a bounded selection
of `QueueItems`: all New/InProgress items plus records whose EndProcessing is
within `QUEUE_HISTORY_LOOKBACK` (default 24h). It requests no queue payload,
reference, input/output data, or exception message. Default OAuth scopes add
`OR.Queues.Read` when collection is enabled; explicit scope lists remain operator-owned.

All metrics below are gauges, per folder and queue ID. The history window is
independent of Grafana's selected time range. Do not apply rate/increase or sum
these snapshots over time. Counts describe item/attempt records, not unique
business transactions. Historical updates and retention can change counts.

| OTel metric | Unit | Meaning |
| --- | --- | --- |
| `uipath.queue.info` | none | Inventory, value 1, queue name attribute |
| `uipath.queue.items` | `{item}` | Current New/InProgress counts, including explicit zero per known queue |
| `uipath.queue.eligible` | `{item}` | New items whose defer date has passed |
| `uipath.queue.deferred` | `{item}` | New items with a future defer date |
| `uipath.queue.oldest_pending_age` | `s` | Oldest eligible age since max(creation, defer date); 0 if none |
| `uipath.queue.overdue` | `{item}` | Active items past DueDate |
| `uipath.queue.transactions` | `{item}` | EndProcessing-window records by current terminal state |
| `uipath.queue.exceptions` | `{item}` | Failed/Retried records by BusinessException, ApplicationException or Unknown |
| `uipath.queue.retry_attempts` | `{item}` | Completed-window records with RetryNumber > 0 |
| `uipath.queue.history_window` | `s` | Configured rolling window |
| `uipath.queue.processing.samples` | `{item}` | Completed-window records with valid processing duration |
| `uipath.queue.processing.mean_duration` | `s` | Mean elapsed processing time; absent with zero samples |
| `uipath.queue.processing.max_duration` | `s` | Maximum elapsed processing time; absent with zero samples |

Prometheus translation replaces dots with underscores and appends `_seconds`
for seconds. Queue names are joined from inventory rather than copied to every
metric. Folder views preserve authorization context. The sample dashboard uses
max per installation/tenant/queue ID to avoid adding shared queue observations
across linked folders. These are sequential snapshots, not an atomic fleet view;
max is an observation policy, not proof of simultaneous global state.

Inventory and item reads must both succeed and validate before the queue snapshot
is saved. A cap, unknown queue/state, invalid identity/timing, or failed read marks
coverage incomplete and suppresses that cycle's queue metrics. Dashboard queries
also gate on current collection success, folder availability and freshness so
previously stored values cannot masquerade as fresh results.

`uipath.collector.dataset.status` is a unitless gauge: 0 disabled, 1 successful,
2 failed, 3 forbidden. Freshness is separate. `uipath.collector.folder.available`
(unit 1, Prometheus `_ratio`) is 1 for currently visible folders and 0 for previously
known/configured folders missing from discovery. Known folder inventory persists
across restarts; missing folders remain selectable and degrade coverage. Deliberate
folder removal requires adjusting the folder allowlist or an operator-reviewed
inventory reset. Discovery failure does not replace the remembered inventory.

References: [queue fields and linked-folder behavior](https://docs.uipath.com/orchestrator/standalone/2025.10/api-guide/queue-items-requests),
[queue item states](https://docs.uipath.com/orchestrator/standalone/2025.10/user-guide/queue-item-statuses).
