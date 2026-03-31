package io.partdb.node.raft;

import jdk.jfr.Category;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("io.partdb.NodeLeaderChange")
@Label("PartDB Node Leader Change")
@Category({"PartDB", "Node"})
final class NodeLeaderChangeEvent extends Event {
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
