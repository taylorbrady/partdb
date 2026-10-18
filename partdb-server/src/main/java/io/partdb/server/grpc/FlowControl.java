package io.partdb.server.grpc;

import io.grpc.stub.ServerCallStreamObserver;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class FlowControl {

    private final ServerCallStreamObserver<?> call;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition ready = lock.newCondition();

    FlowControl(ServerCallStreamObserver<?> call) {
        this.call = call;
        call.setOnReadyHandler(this::signal);
        call.setOnCancelHandler(this::signal);
    }

    boolean awaitReady() {
        lock.lock();
        try {
            while (!call.isReady() && !call.isCancelled()) {
                ready.awaitUninterruptibly();
            }
            return !call.isCancelled();
        } finally {
            lock.unlock();
        }
    }

    private void signal() {
        lock.lock();
        try {
            ready.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
