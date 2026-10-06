# Telemetry contract

The producer emits OTLP/HTTP protobuf using generated OTLP Go definitions v1.9.0.
It implements separate `/v1/metrics`, `/v1/logs` and `/v1/traces` requests to one
base endpoint. Transport responses are checked before source outbox deletion.

Semantic baseline: OpenTelemetry Semantic Conventions **v1.43.0**. Standard
resource attributes used here are stable `service.name`, `service.namespace`,
`service.instance.id` and
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
Collector preserves OTLP to Thanos without resource promotion. Standard mapping
produces `job` from `service.namespace/service.name` and `instance` from
`service.instance.id`. Source metadata remains on `target_info`; consumers
join it on `(job, instance)` before domain-specific filtering and aggregation.
Counter rates are calculated before joining metadata.

`service.namespace=uipath` groups the two logical service roles. The instance ID
is a UUIDv5 using the SemConv namespace and the JSON tuple
`["uipath-source-v1", installation, tenant, environment]`. It identifies the
configured logical Orchestrator tenant and its single polling authority, not an
individual Orchestrator node, robot or collector process. The two roles share
this source ID but have distinct service names; logs and spans use the same
identity as domain metrics. Identity is stable across restarts to preserve
persisted cumulative series, and distinct across sources and environments.
Installation identifiers must be unique within the monitoring estate. Concurrent
collectors for the same source remain unsupported.

Changing to this contract creates new Prometheus series. Old promoted series and
queued pre-upgrade OTLP batches keep their previous identity; consumer queries
should not fall back to ambiguous old series. Drain old outboxes and coordinate the
adapter and dashboard rollout; short rate windows need fresh samples. Stored history
is not rewritten. Generic platform promotion of environment metadata is optional;
the sample stack needs no promotion flags at all.

Robot logs use source TimeStamp and collection ObservedTime, native severity,
an optional message body and `uipath.*` identifiers. Job lifecycle events use job
end time. Messages are off by default; enabling them may expose source payload
content to the configured backend. Input/output arguments, queue payloads,
Windows identities and RawMessage are not fetched.

### Log identity opt-in

`INCLUDE_LOG_RECORD_UID=true` adds `log.record.uid` to job-completion and robot
log records. SemConv v1.43.0 defines this log attribute as **Development** and
**Opt-In**, so it is off by default; the synthetic Compose demo enables it.
The existing `uipath.event.id` remains available to older consumers.

The UID is lowercase SHA-256 hex of the JSON string tuple
`["uipath-log-v1", installation, tenant, environment, folderID, sourceEventID]`.
The source event ID distinguishes job and robot records. It does not depend on
message content or collection time. Identical messages from distinct source
records remain distinct; repeat collection and outbox replay keep the same UID.
Source IDs are subject to the adapter's existing validation and reuse limits.
See the [attribute definition](https://github.com/open-telemetry/semantic-conventions/blob/v1.43.0/model/log/registry.yaml#L60)
and [log requirement level](https://github.com/open-telemetry/semantic-conventions/blob/v1.43.0/model/log/common.yaml#L7).

When enabled, delivery also adds UIDs to older queued log batches using the
identity stored in each batch. Current process configuration never substitutes
for queued source identity. Existing UIDs and other telemetry fields are retained;
retryable delivery failures persist upgraded batches. A crash between receiver
acceptance and local acknowledgment deterministically reproduces the same UID.
Batches lacking installation, tenant, environment, folder or source event ID
remain pending with an error; identity is never guessed from current settings.
Enabling this option does not replay quarantined batches or reset checkpoints.

Enable UID emission and verify the receiver sees it before switching downstream
deduplication to this attribute. Receivers own any document-ID conversion. Old
documents created using a different ID scheme are not retroactively deduplicated,
and backend rollover/retention semantics can still allow duplicates. Keep the
opt-in enabled for the lifetime of this delivery contract, including rollbacks.

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
metric. Folder views preserve authorization context. Consumers should use
max per installation/tenant/queue ID to avoid adding shared queue observations
across linked folders. These are sequential snapshots, not an atomic fleet view;
max is an observation policy, not proof of simultaneous global state.

Inventory and item reads must both succeed and validate before the queue snapshot
is saved. A cap, unknown queue/state, invalid identity/timing, or failed read marks
coverage incomplete and suppresses that cycle's queue metrics. Consumer queries
should also gate on current collection success, folder availability and freshness so
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

## Host occupancy and active-job snapshots

`COLLECT_RUNTIMES` polls the tenant-level
`/odata/Sessions/UiPath.Server.Configuration.OData.GetMachineSessionRuntimes`
once per cycle, with bounded pagination and `OR.Robots.Read`. Capacity is never
summed over folder views. The API returns one row per machine session and
runtime type, including every type without slots, and repeats connection state
and heartbeat on each row. The adapter therefore emits two levels:

- Session series carry source identity, `uipath.session.id`, `uipath.machine.id`
  and `uipath.host.name`, once per session.
- Runtime series add `uipath.runtime.type` and are emitted only for types with at
  least one slot. In a fresh, successful inventory, a missing type means zero
  slots, not an unknown state.

Machine ID is a UiPath template/object identity, not a physical host ID.
`uipath.host.name` describes the remote execution target; the logical
Orchestrator resource does not acquire `host.*` attributes. These are custom
attributes, not new claims of stable OTel semantic conventions.

| Metric | Unit | Meaning |
| --- | --- | --- |
| `uipath.session.status` | none | 0 unknown/stale, 1 free, 2 partly occupied, 3 occupied, 4 maintenance, 5 disconnected, 6 unresponsive, 7 no slots; occupancy over all runtime types |
| `uipath.session.last_heartbeat`, `uipath.session.observed_time` | `s` | Oldest source heartbeat of the session and successful observation Unix timestamps |
| `uipath.runtime.status` | none | Same codes; occupancy of this runtime type, or the session's state when it is 0 or 4–6 |
| `uipath.runtime.capacity`, `uipath.runtime.used` | `{slot}` | Assigned and consumed runtime slots of types with capacity; gauges |
| `uipath.runtime.observed_time` | `s` | Successful observation Unix timestamp of the runtime row |
| `uipath.runtime.observed` | `{runtime}` | Rows in the successful tenant inventory, including zero-capacity types |
| `uipath.session.observed` | `{session}` | Machine sessions in the successful tenant inventory |
| `uipath.collector.runtimes.status` | none | 0 disabled, 1 successful, 2 failed, 3 forbidden |
| `uipath.collector.runtimes.last_success_time` | `s` | Last successful tenant observation, persisted across restart |
| `uipath.collector.freshness` | `s` | Configured freshness budget |

Session state merges a session's rows conservatively: maintenance, disconnected,
unresponsive or unknown on any row applies to the session, and the oldest
heartbeat wins. Maintenance, disconnected and unresponsive states take priority
over heartbeat age and occupancy. A Busy session without used slots is unknown;
an idle runtime type on a Busy session with used slots elsewhere is free.
Missing/invalid counts or identities, duplicates, rows of one session with
different machine IDs or host names, unknown API enums, invalid heartbeat
timestamps and capped reads invalidate the inventory.
No old capacity is re-exported on failure. A successful read with an old heartbeat
cannot label the host free. Consumer queries must match the latest successful
observation and require current collection success/freshness, so disappeared
hosts do not survive through Prometheus lookback as available capacity.

Jobs request `HostMachineName`, `JobPriority`, `RuntimeType`, `SourceType` and a
narrow Robot expansion. Completion logs and spans receive the assignment fields
when present. `Robot.Username` is requested and emitted only with
`INCLUDE_ROBOT_USERNAMES`; accounts and job keys never become metric dimensions.
An absent host is unassigned/unknown, not an idle host. Source type is an API
classification, not evidence of who initiated the job.

`COLLECT_JOB_SNAPSHOTS` produces one OTLP log per poll for all allowed folders:
`uipath.event.kind=jobs.snapshot`, `uipath.folder.id=_all`, observed timestamp,
`uipath.snapshot.status`, integer `uipath.snapshot.jobs`, and RFC3339
`uipath.snapshot.valid_until` (observation plus `METRIC_FRESHNESS`). The body is a
JSON object keyed by `folder-id/job-key`; each value contains process, named
folder, host, robot, optional account, state, priority, runtime and timestamps.
Elapsed/waiting seconds are sampled wall time, not consumed runtime.

An empty complete body `{}` clears previous assignments. A discovery or job-read
failure, missing previously visible/configured folder, invalid job record, or
cap produces an incomplete/capped snapshot with `{}`; count zero then does not
establish emptiness. Limits are 1,000 active jobs and 1 MiB body. Consumers select
the newest source observation before testing status; do not query only complete
records. Query unexpired records and require complete status before
interpreting the body. Avoid decreasing the freshness budget while older
snapshots remain valid, since it can change expiration order.

Snapshots use the existing durable OTLP log outbox, independently of robot-log
collection. Their event IDs contain observation time; opt-in `log.record.uid`
remains unchanged on persisted replay. Sequential API reads cannot guarantee a
transactionally consistent fleet view, and runtime capacity may include work in
folders the client cannot read. The snapshot scope is only the allowed/visible
folders. Schedules and queue-to-job attribution require separate source contracts.
