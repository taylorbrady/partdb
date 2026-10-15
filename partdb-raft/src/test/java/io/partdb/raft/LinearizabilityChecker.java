package io.partdb.raft;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

final class LinearizabilityChecker {

    record Operation(String key, boolean write, String value, long call, long ret) {
        Operation {
            Objects.requireNonNull(key, "key must not be null");
            if (ret < call) {
                throw new IllegalArgumentException("operation returns before it is called");
            }
        }

        static Operation write(String key, String value, long call, long ret) {
            return new Operation(key, true, value, call, ret);
        }

        static Operation read(String key, String observed, long call, long ret) {
            return new Operation(key, false, observed, call, ret);
        }
    }

    private LinearizabilityChecker() {
    }

    static Optional<String> violation(List<Operation> history) {
        var byKey = history.stream().collect(Collectors.groupingBy(Operation::key));
        for (var entry : byKey.entrySet()) {
            if (!linearizable(entry.getValue())) {
                return Optional.of(describe(entry.getKey(), entry.getValue()));
            }
        }
        return Optional.empty();
    }

    private static boolean linearizable(List<Operation> operations) {
        Event head = link(operations);
        Event entry = head.next;
        String state = null;
        BitSet linearized = new BitSet();
        Set<CacheKey> cache = new HashSet<>();
        var stack = new ArrayDeque<Frame>();

        while (head.next != null) {
            if (entry.call) {
                Operation op = entry.operation;
                if (op.write() || Objects.equals(op.value(), state)) {
                    String next = op.write() ? op.value() : state;
                    var candidate = (BitSet) linearized.clone();
                    candidate.set(entry.id);
                    if (cache.add(new CacheKey(candidate, next))) {
                        stack.push(new Frame(entry, state));
                        state = next;
                        linearized = candidate;
                        entry.lift();
                        entry = head.next;
                        continue;
                    }
                }
                entry = entry.next;
            } else {
                if (stack.isEmpty()) {
                    return false;
                }
                var frame = stack.pop();
                state = frame.state();
                linearized = (BitSet) linearized.clone();
                linearized.clear(frame.call().id);
                frame.call().unlift();
                entry = frame.call().next;
            }
        }
        return true;
    }

    private static Event link(List<Operation> operations) {
        var events = new ArrayList<Event>();
        for (int id = 0; id < operations.size(); id++) {
            Operation op = operations.get(id);
            var call = new Event(id, true, op, op.call());
            var ret = new Event(id, false, op, op.ret());
            call.match = ret;
            events.add(call);
            events.add(ret);
        }
        events.sort(Comparator.comparingLong((Event e) -> e.time).thenComparing(e -> !e.call));

        var head = new Event(-1, false, null, Long.MIN_VALUE);
        Event previous = head;
        for (Event event : events) {
            previous.next = event;
            event.prev = previous;
            previous = event;
        }
        return head;
    }

    private static String describe(String key, List<Operation> operations) {
        return operations.stream()
            .sorted(Comparator.comparingLong(Operation::call))
            .map(op -> "  [%d, %d] %s %s".formatted(op.call(), op.ret(), op.write() ? "write" : "read ", op.value()))
            .collect(Collectors.joining("\n", "history for key " + key + " is not linearizable:\n", ""));
    }

    private record CacheKey(BitSet linearized, String state) {}

    private record Frame(Event call, String state) {}

    private static final class Event {
        private final int id;
        private final boolean call;
        private final Operation operation;
        private final long time;
        private Event match;
        private Event prev;
        private Event next;

        private Event(int id, boolean call, Operation operation, long time) {
            this.id = id;
            this.call = call;
            this.operation = operation;
            this.time = time;
        }

        private void lift() {
            prev.next = next;
            if (next != null) {
                next.prev = prev;
            }
            match.prev.next = match.next;
            if (match.next != null) {
                match.next.prev = match.prev;
            }
        }

        private void unlift() {
            match.prev.next = match;
            if (match.next != null) {
                match.next.prev = match;
            }
            prev.next = this;
            if (next != null) {
                next.prev = this;
            }
        }
    }
}
