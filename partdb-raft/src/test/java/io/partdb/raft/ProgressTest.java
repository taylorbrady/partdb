package io.partdb.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProgressTest {

    @Test
    void probePausesAfterOneSend() {
        var progress = new Progress(5, 4);

        progress.sent(5, true);

        assertTrue(progress.isPaused());
        assertEquals(5, progress.next());
        progress.resume();
        assertFalse(progress.isPaused());
    }

    @Test
    void replicateAdvancesOptimisticallyUntilInflightIsFull() {
        var progress = new Progress(5, 2);
        progress.maybeUpdate(4);
        progress.becomeReplicate();

        progress.sent(6, true);
        progress.sent(8, true);

        assertEquals(9, progress.next());
        assertTrue(progress.isPaused());
        progress.acknowledged(6);
        assertFalse(progress.isPaused());
    }

    @Test
    void probeRejectionUsesHintButNeverDropsBelowMatch() {
        var progress = new Progress(11, 4);
        progress.maybeUpdate(3);
        progress.becomeProbe();
        progress.sent(10, true);
        assertEquals(4, progress.next());

        assertFalse(progress.maybeDecrease(9, 1));
        assertTrue(progress.maybeDecrease(3, 1));
        assertEquals(4, progress.next());
    }

    @Test
    void probeRejectionFollowsHint() {
        var progress = new Progress(11, 4);

        assertTrue(progress.maybeDecrease(10, 6));

        assertEquals(7, progress.next());
        assertFalse(progress.isPaused());
    }

    @Test
    void replicateRejectionFallsBackToMatch() {
        var progress = new Progress(1, 4);
        progress.maybeUpdate(5);
        progress.becomeReplicate();
        progress.sent(9, true);

        assertFalse(progress.maybeDecrease(4, 0));
        assertTrue(progress.maybeDecrease(7, 0));
        assertEquals(6, progress.next());
    }

    @Test
    void snapshotStateResumesAfterPendingSnapshot() {
        var progress = new Progress(1, 4);
        progress.becomeSnapshot(20);
        assertTrue(progress.isPaused());

        progress.becomeProbe();

        assertEquals(21, progress.next());
        assertFalse(progress.isPaused());
    }

    @Test
    void abortedSnapshotRestartsFromMatch() {
        var progress = new Progress(1, 4);
        progress.maybeUpdate(3);
        progress.becomeSnapshot(20);

        progress.abortSnapshot();

        assertEquals(4, progress.next());
    }
}
