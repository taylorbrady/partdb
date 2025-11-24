package io.partdb.common;

public record Lease(
    long id,
    long ttlMillis,
    long grantedAtMillis
) {
    public Lease {
        if (id <= 0) {
            throw new IllegalArgumentException("id must be positive");
        }
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttlMillis must be positive");
        }
        if (grantedAtMillis < 0) {
            throw new IllegalArgumentException("grantedAtMillis must be non-negative");
        }
    }

    public boolean isExpired(long currentTimeMillis) {
        return currentTimeMillis >= grantedAtMillis + ttlMillis;
    }

    public Lease renew(long currentTimeMillis) {
        return new Lease(id, ttlMillis, currentTimeMillis);
    }

    public long expiresAtMillis() {
        return grantedAtMillis + ttlMillis;
    }
}
