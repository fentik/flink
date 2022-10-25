package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.MaskUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.LinkedHashSet;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer for {@link LinkedHashSet}. The serializer relies on an element serializer for the
 * serialization of the set's elements.
 *
 * @param <T> The type of element in the set.
 */
public class LinkedHashSetSerializer <T> extends TypeSerializer<LinkedHashSet<T>> {

    // legacy, don't touch until we drop support for 1.9 savepoints
    private static final long serialVersionUID = 1L;

    // The serializer for the elements of the set.
    private final TypeSerializer<T> elementSerializer;

    private final boolean hasNullMask;
    private transient boolean[] reuseMask;

    /**
     * Creates a set serializer that uses the given serializer to serialize the set's elements.
     *
     * @param elementSerializer The serializer for the elements of the set
     */
    public LinkedHashSetSerializer(TypeSerializer<T> elementSerializer) {
        this(elementSerializer, true);
    }

    public LinkedHashSetSerializer(TypeSerializer<T> elementSerializer, boolean hasNullMask) {
        this.elementSerializer = checkNotNull(elementSerializer);
        this.hasNullMask = hasNullMask;
    }

    // ------------------------------------------------------------------------
    //  LinkedHashSetSerializer specific properties
    // ------------------------------------------------------------------------

    /**
     * Gets the serializer for the elements of the set.
     *
     * @return The serializer for the elements of the set
     */
    public TypeSerializer<T> getElementSerializer() {
        return elementSerializer;
    }

    // ------------------------------------------------------------------------
    //  Type Serializer implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<LinkedHashSet<T>> duplicate() {
        return new LinkedHashSetSerializer<>(elementSerializer.duplicate(), hasNullMask);
    }

    @Override
    public LinkedHashSet<T> createInstance() {
        return new LinkedHashSet<>();
    }

    @Override
    public LinkedHashSet<T> copy(LinkedHashSet<T> from) {
        LinkedHashSet<T> newSet = new LinkedHashSet<>();
        for (T element : from) {
            // there is no compatibility problem here as it only copies from memory to memory
            if (element == null) {
                newSet.add(null);
            } else {
                newSet.add(elementSerializer.copy(element));
            }
        }
        return newSet;
    }

    @Override
    public LinkedHashSet<T> copy(LinkedHashSet<T> from, LinkedHashSet<T> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1; // var length
    }

    private void ensureReuseMaskLength(int len) {
        if (reuseMask == null || reuseMask.length < len) {
            reuseMask = new boolean[len];
        }
    }

    @Override
    public void serialize(LinkedHashSet<T> set, DataOutputView target) throws IOException {
        target.writeInt(set.size());
        if (hasNullMask) {
            ensureReuseMaskLength(set.size());
            MaskUtils.writeMask(getNullMask(set), set.size(), target);
        }
        for (T element : set) {
            if (element != null) {
                elementSerializer.serialize(element, target);
            }
        }
    }

    private boolean[] getNullMask(LinkedHashSet<T> set) {
        int idx = 0;
        for (T item : set) {
            reuseMask[idx] = item == null;
            idx++;
        }
        return reuseMask;
    }

    @Override
    public LinkedHashSet<T> deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();
        final LinkedHashSet<T> set = new LinkedHashSet<>();
        if (hasNullMask) {
            ensureReuseMaskLength(size);
            MaskUtils.readIntoMask(source, reuseMask, size);
        }
        for (int i = 0; i < size; i++) {
            if (hasNullMask && reuseMask[i]) {
                set.add(null);
            } else {
                set.add(elementSerializer.deserialize(source));
            }
        }
        return set;
    }

    @Override
    public LinkedHashSet<T> deserialize(LinkedHashSet<T> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        // copy number of elements
        final int num = source.readInt();
        target.writeInt(num);
        if (hasNullMask) {
            ensureReuseMaskLength(num);
            MaskUtils.readIntoAndCopyMask(source, target, reuseMask, num);
        }
        for (int i = 0; i < num; i++) {
            if (!(hasNullMask && reuseMask[i])) {
                elementSerializer.copy(source, target);
            }
        }
    }

    // --------------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == getClass()
                        && elementSerializer.equals(
                                ((LinkedHashSetSerializer<?>) obj).elementSerializer));
    }

    @Override
    public int hashCode() {
        return elementSerializer.hashCode();
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshot & compatibility
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<LinkedHashSet<T>> snapshotConfiguration() {
        return new LinkedHashSetSerializerSnapshot<>(this);
    }

    /** Snapshot class for the {@link LinkedHashSetSerializer}. */
    public static class LinkedHashSetSerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<LinkedHashSet<T>, LinkedHashSetSerializer<T>> {

        private static final int CURRENT_VERSION = 3;

        private static final int FIRST_VERSION_WITH_NULL_MASK = 2;

        private boolean hasNullMask = true;

        /** Constructor for read instantiation. */
        public LinkedHashSetSerializerSnapshot() {
            super(LinkedHashSetSerializer.class);
        }

        /** Constructor to create the snapshot for writing. */
        public LinkedHashSetSerializerSnapshot(LinkedHashSetSerializer<T> setSerializer) {
            super(setSerializer);
            this.hasNullMask = setSerializer.hasNullMask;
        }

        @Override
        public int getCurrentOuterSnapshotVersion() {
            return CURRENT_VERSION;
        }

        @Override
        protected void readOuterSnapshot(
                int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            if (readOuterSnapshotVersion < FIRST_VERSION_WITH_NULL_MASK) {
                hasNullMask = false;
            } else if (readOuterSnapshotVersion == FIRST_VERSION_WITH_NULL_MASK) {
                hasNullMask = true;
            } else {
                hasNullMask = in.readBoolean();
            }
        }

        @Override
        protected void writeOuterSnapshot(DataOutputView out) throws IOException {
            out.writeBoolean(hasNullMask);
        }

        @Override
        protected OuterSchemaCompatibility resolveOuterSchemaCompatibility(
                LinkedHashSetSerializer<T> newSerializer) {
            if (hasNullMask != newSerializer.hasNullMask) {
                return OuterSchemaCompatibility.COMPATIBLE_AFTER_MIGRATION;
            }
            return OuterSchemaCompatibility.COMPATIBLE_AS_IS;
        }

        @Override
        protected LinkedHashSetSerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            @SuppressWarnings("unchecked")
            TypeSerializer<T> elementSerializer = (TypeSerializer<T>) nestedSerializers[0];
            return new LinkedHashSetSerializer<>(elementSerializer, hasNullMask);
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                LinkedHashSetSerializer<T> outerSerializer) {
            return new TypeSerializer<?>[] {outerSerializer.getElementSerializer()};
        }
    }
}
