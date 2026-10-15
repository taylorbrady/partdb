package io.partdb.raft.runtime;

import jdk.jfr.Category;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("io.partdb.LeaderChange")
@Label("PartDB Leader Change")
@Category({"PartDB", "Raft"})
final class LeaderChangeEvent extends Event {
    @Label("Node ID")
    String nodeId;

    @Label("Term")
    long term;

    @Label("Previous Role")
    String previousRole;

    @Label("New Role")
    String newRole;

    @Label("Previous Leader ID")
    String previousLeaderId;

    @Label("New Leader ID")
    String newLeaderId;
}
