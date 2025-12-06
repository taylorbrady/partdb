package io.partdb.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class PendingRequestsTest {

    private PendingRequests pending;

    @BeforeEach
    void setUp() {
        pending = new PendingRequests();
    }

    @Test
    void trackReturnsUniqueRequestIds() {
        var tracked1 = pending.track();
        var tracked2 = pending.track();
        var tracked3 = pending.track();

        assertNotEquals(tracked1.requestId(), tracked2.requestId());
        assertNotEquals(tracked2.requestId(), tracked3.requestId());
    }

    @Test
    void trackReturnsFutureThatIsNotCompleted() {
        var tracked = pending.track();

        assertFalse(tracked.future().isDone());
    }

    @Test
    void completeResolvesFuture() {
        var tracked = pending.track();

        pending.complete(tracked.requestId());

        assertTrue(tracked.future().isDone());
        assertFalse(tracked.future().isCompletedExceptionally());
    }

    @Test
    void completeWithUnknownIdDoesNothing() {
        var tracked = pending.track();

        pending.complete(999999L);

        assertFalse(tracked.future().isDone());
    }

    @Test
    void cancelRemovesRequestWithoutCompletingFuture() {
        var tracked = pending.track();

        pending.cancel(tracked.requestId());

        assertFalse(tracked.future().isDone());
        assertEquals(0, pending.size());
    }

    @Test
    void sizeReflectsPendingCount() {
        assertEquals(0, pending.size());

        var tracked1 = pending.track();
        assertEquals(1, pending.size());

        var tracked2 = pending.track();
        assertEquals(2, pending.size());

        pending.complete(tracked1.requestId());
        assertEquals(1, pending.size());

        pending.complete(tracked2.requestId());
        assertEquals(0, pending.size());
    }

    @Test
    void completedFutureIsRemovedFromPending() {
        var tracked = pending.track();
        assertEquals(1, pending.size());

        pending.complete(tracked.requestId());

        assertEquals(0, pending.size());
    }

    @Test
    void multipleRequestsAreIndependent() {
        var tracked1 = pending.track();
        var tracked2 = pending.track();
        var tracked3 = pending.track();

        pending.complete(tracked2.requestId());

        assertFalse(tracked1.future().isDone());
        assertTrue(tracked2.future().isDone());
        assertFalse(tracked3.future().isDone());
    }

    @Test
    void futureCanBeAwaitedBeforeCompletion() throws Exception {
        var tracked = pending.track();
        var completed = new AtomicBoolean(false);
        var latch = new CountDownLatch(1);

        Thread.ofVirtual().start(() -> {
            tracked.future().join();
            completed.set(true);
            latch.countDown();
        });

        Thread.sleep(10);
        assertFalse(completed.get());

        pending.complete(tracked.requestId());

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertTrue(completed.get());
    }

    @Test
    void doubleCompleteIsIgnored() {
        var tracked = pending.track();

        pending.complete(tracked.requestId());
        pending.complete(tracked.requestId());

        assertTrue(tracked.future().isDone());
    }
}
