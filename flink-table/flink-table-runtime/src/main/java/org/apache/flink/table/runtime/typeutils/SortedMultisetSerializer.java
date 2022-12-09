/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.shaded.guava30.com.google.common.collect.Multiset;
import org.apache.flink.shaded.guava30.com.google.common.collect.SortedMultiset;
import org.apache.flink.shaded.guava30.com.google.common.collect.TreeMultiset;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import java.util.Comparator;

/**
 * A serializer for {@link SortedMultiset}. The serializer relies on a key serializer and a value
 * serializer for the serialization of the map's key-value pairs. It also deploys a comparator to
 * ensure the order of the keys.
 *
 * <p>The serialization format for the map is as follows: four bytes for the length of the map,
 * followed by the serialized representation of each key-value pair. To allow null values, each
 * value is prefixed by a null flag.
 *
 * @param <T> The type of the values in the map.
 */
public final class SortedMultisetSerializer<T> extends TypeSerializer<SortedMultiset<T>> {

    private static final long serialVersionUID = 1L;

    /** The comparator for the keys in the map. */
    private final Comparator<T> comparator;
    private final TypeSerializer<T> valueSerializer;

    /**
     * Constructor with given comparator, and the serializers for the keys and values in the map.
     *
     * @param comparator The comparator for the keys in the map.
     * @param keySerializer The serializer for the keys in the map.
     * @param valueSerializer The serializer for the values in the map.
     */
    public SortedMultisetSerializer(
            Comparator<T> comparator,
            TypeSerializer<T> valueSerializer) {
        this.comparator = comparator;
        this.valueSerializer = valueSerializer;
    }

    public TypeSerializer<T> getValueSerializer() {
        return valueSerializer;
    }

    /**
     * Returns the comparator for the keys in the map.
     *
     * @return The comparator for the keys in the map.
     */
    public Comparator<T> getComparator() {
        return comparator;
    }

    @Override
    public TypeSerializer<SortedMultiset<T>> duplicate() {
        TypeSerializer<T> vsDup = valueSerializer.duplicate();
        return new SortedMultisetSerializer<T>(comparator, vsDup);
    }

    @Override
    public String toString() {
        return "SortedMultisetSerializer{"
                + "comparator = "
                + comparator
                + ", valueSerializer = "
                + valueSerializer
                + "}";
    }

   // ------------------------------------------------------------------------
    //  Type Serializer implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TreeMultiset<T> createInstance() {
        return TreeMultiset.create(comparator);
    }

    public SortedMultiset<T> copy(SortedMultiset<T> from) {
        TreeMultiset newSet = createInstance();
        for (Multiset.Entry<T> entry : from.entrySet()) {
            T newValue = entry.getElement() == null ? null : valueSerializer.copy(entry.getElement());

            newSet.add(newValue);
        }

        return newSet;
    }

    public SortedMultiset<T> copy(SortedMultiset<T> from, SortedMultiset<T> reuse) {
        return copy(from);
    }

    public int getLength() {
        return -1; // var length
    }

    public void serialize(SortedMultiset<T> set, DataOutputView target) throws IOException {
        final int size = set.size();
        target.writeInt(size);

        for (Multiset.Entry<T> entry : set.entrySet()) {
            valueSerializer.serialize(entry.getElement(), target);
        }
    }

    public SortedMultiset<T> deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();

        TreeMultiset<T> set = TreeMultiset.create(comparator);

        for (int i = 0; i < size; ++i) {
            T value = valueSerializer.deserialize(source);
            set.add(value);
        }

        return set;
    }

    public SortedMultiset<T> deserialize(SortedMultiset<T> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        final int size = source.readInt();
        target.writeInt(size);

        for (int i = 0; i < size; ++i) {
            valueSerializer.copy(source, target);
        }
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SortedMultisetSerializer<?> other = (SortedMultisetSerializer<?>) o;
        return comparator.equals(other.comparator)
            && valueSerializer.equals(other.valueSerializer);
    }

    @Override
    public int hashCode() {
        int result = valueSerializer.hashCode() + comparator.hashCode();
        return result;
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshot
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<SortedMultiset<T>> snapshotConfiguration() {
        return new SortedMultisetSerializerSnapshot<>(this);
    }
}
