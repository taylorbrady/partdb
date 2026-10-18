package io.partdb.server;

import io.partdb.node.Node;
import io.partdb.raft.Role;

import java.time.Duration;
import java.util.Locale;

final class PrometheusMetrics {

    static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private final Node node;

    PrometheusMetrics(Node node) {
        this.node = node;
    }

    String scrape() {
        var status = node.status();
        var metrics = node.metrics();
        var storage = metrics.storage();
        var out = new Exposition();

        out.family("partdb_node_info", "gauge", "Identity of this PartDB node.");
        out.sample("partdb_node_info", "node_id", status.nodeId().value(), 1);
        out.gauge("partdb_node_running", "Whether the node's Raft runtime is running.", status.running() ? 1 : 0);

        out.family("partdb_raft_role", "gauge", "Current Raft role of this node, one series per role.");
        for (Role role : Role.values()) {
            out.sample("partdb_raft_role", "role", role.name().toLowerCase(Locale.ROOT), role == status.role() ? 1 : 0);
        }
        out.gauge("partdb_raft_has_leader", "Whether this node knows the current leader.", status.leader().isPresent() ? 1 : 0);
        out.gauge("partdb_raft_term", "Current Raft term.", status.term());
        out.gauge("partdb_raft_commit_index", "Highest log index known to be committed.", status.commitIndex());
        out.gauge("partdb_raft_applied_index", "Highest log index applied to the state machine.", status.appliedIndex());
        out.gauge("partdb_raft_snapshot_index", "Log index of the latest snapshot.", status.snapshotIndex());
        status.lastLeaderChange().ifPresent(changed -> out.gauge("partdb_raft_last_leader_change_timestamp_seconds",
            "Unix time at which this node last saw the leader change.", changed.toEpochMilli() / 1000.0));

        out.counter("partdb_proposals_total", "Write transactions proposed through this node.", metrics.proposals());
        out.counter("partdb_proposals_failed_total", "Proposed write transactions that failed or have an unknown outcome.", metrics.failedProposals());

        out.gauge("partdb_storage_active_memtable_bytes", "Bytes held by the active memtable.", storage.activeMemtableBytes());
        out.gauge("partdb_storage_immutable_memtables", "Memtables waiting to be flushed.", storage.immutableMemtableCount());
        out.gauge("partdb_storage_tables", "Sorted table files in the current version.", storage.tableCount());
        out.gauge("partdb_storage_table_bytes", "Bytes in sorted table files.", storage.tableBytes());
        out.gauge("partdb_storage_active_compactions", "Compactions in progress.", storage.activeCompactions());
        out.counter("partdb_storage_compactions_total", "Compactions completed.", storage.completedCompactions());
        out.counter("partdb_storage_compactions_failed_total", "Compactions that failed.", storage.failedCompactions());
        out.gauge("partdb_storage_last_compaction_duration_seconds", "Duration of the most recent compaction.", seconds(storage.lastCompactionDuration()));
        out.counter("partdb_storage_checkpoints_total", "Storage checkpoints written for snapshots.", storage.checkpointCount());
        out.counter("partdb_storage_restores_total", "Storage restores from snapshots.", storage.restoreCount());
        out.gauge("partdb_storage_last_checkpoint_duration_seconds", "Duration of the most recent checkpoint.", seconds(storage.lastCheckpointDuration()));
        out.gauge("partdb_storage_last_restore_duration_seconds", "Duration of the most recent restore.", seconds(storage.lastRestoreDuration()));
        return out.toString();
    }

    private static double seconds(Duration duration) {
        return duration.toNanos() / 1e9;
    }

    private static final class Exposition {
        private final StringBuilder text = new StringBuilder();

        void gauge(String name, String help, double value) {
            family(name, "gauge", help);
            sample(name, value);
        }

        void counter(String name, String help, double value) {
            family(name, "counter", help);
            sample(name, value);
        }

        void family(String name, String type, String help) {
            text.append("# HELP ").append(name).append(' ').append(help).append('\n');
            text.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        }

        void sample(String name, double value) {
            text.append(name).append(' ').append(number(value)).append('\n');
        }

        void sample(String name, String label, String labelValue, double value) {
            text.append(name).append('{').append(label).append("=\"").append(escape(labelValue)).append("\"} ")
                .append(number(value)).append('\n');
        }

        @Override
        public String toString() {
            return text.toString();
        }

        private static String number(double value) {
            return value == Math.rint(value) && Math.abs(value) < 1e15 ? Long.toString((long) value) : Double.toString(value);
        }

        private static String escape(String value) {
            return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
        }
    }
}
