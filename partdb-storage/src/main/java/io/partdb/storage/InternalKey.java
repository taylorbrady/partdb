package io.partdb.storage;

record InternalKey(Slice userKey, long revision) implements Comparable<InternalKey> {

    static InternalKey first(Slice userKey) {
        return new InternalKey(userKey, Long.MAX_VALUE);
    }

    @Override
    public int compareTo(InternalKey other) {
        int byKey = userKey.compareTo(other.userKey);
        return byKey != 0 ? byKey : Long.compare(other.revision, revision);
    }
}
