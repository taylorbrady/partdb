package io.partdb.consensus;

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

    @Label("Previous RaftRole")
    String previousRole;

    @Label("New RaftRole")
    String newRole;

    @Label("Previous Leader ID")
    String previousLeaderId;

    @Label("New Leader ID")
    String newLeaderId;
}
