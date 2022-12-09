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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.shaded.guava30.com.google.common.collect.SortedMultiset;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Comparator;

/**
 * The type information for sorted multisets.
 *
 * @param <T> The type of the values in the multiset.
 */
@PublicEvolving
public class SortedMultisetTypeInfo<T> extends TypeInformation<SortedMultiset<T>> {

    private static final long serialVersionUID = 1L;

    /** The comparator for the keys in the map. */
    private final Comparator<T> comparator;
    private final TypeInformation<T> valueTypeInfo;

    public SortedMultisetTypeInfo(
            TypeInformation<T> valueTypeInfo,
            Comparator<T> comparator) {
        Preconditions.checkNotNull(comparator, "The comparator cannot be null.");
        this.comparator = comparator;
        this.valueTypeInfo = valueTypeInfo;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        return 2;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public Class<SortedMultiset<T>> getTypeClass() {
        return (Class<SortedMultiset<T>>) (Class<?>) SortedMultiset.class;
    }

    public TypeSerializer<SortedMultiset<T>> createSerializer(ExecutionConfig config) {
        TypeSerializer<T> valueTypeSerializer = valueTypeInfo.createSerializer(config);

        return new SortedMultisetSerializer<>(comparator, valueTypeSerializer);
    }

    public boolean canEqual(Object obj) {
        return null != obj && getClass() == obj.getClass();
    }

    public boolean equals(Object o) {
        SortedMultisetTypeInfo<?> that = (SortedMultisetTypeInfo<?>) o;

        return comparator.equals(that.comparator);
    }

    @Override
    public int hashCode() {
        int result = 31 * comparator.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SortedMultisetTypeInfo{"
                + "comparator="
                + comparator
                + ", valueTypeInfo="
                + valueTypeInfo
                + "}";
    }

    // --------------------------------------------------------------------------

    /** The default comparator for comparable types. */
    private static class ComparableComparator<K> implements Comparator<K>, Serializable {
        private static final long serialVersionUID = 1L;

        @SuppressWarnings("unchecked")
        public int compare(K obj1, K obj2) {
            return ((Comparable<K>) obj1).compareTo(obj2);
        }

        @Override
        public boolean equals(Object o) {
            return (o == this) || (o != null && o.getClass() == getClass());
        }

        @Override
        public int hashCode() {
            return "ComparableComparator".hashCode();
        }
    }
}
