/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Utility class for tiering operations
 *
 * @opensearch.internal
 */
public class TieringUtils {

    /**
     *  Checks if the specified shard is a partial shard by
     *  checking the INDEX_STORE_LOCALITY_SETTING for its index.
     *  see {@link #isPartialIndex(IndexMetadata)}
     * @param shard ShardRouting object representing the shard
     * @param allocation RoutingAllocation object representing the allocation
     * @return true if the shard is a partial shard, false otherwise
     */
    public static boolean isPartialShard(ShardRouting shard, RoutingAllocation allocation) {
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shard.index());
        return isPartialIndex(indexMetadata);
    }

    /**
     * Checks if the specified index is a partial index by
     * checking the INDEX_STORE_LOCALITY_SETTING for the index.
     *
     * @param indexMetadata the metadata of the index
     * @return true if the index is a partial index, false otherwise
     */
    public static boolean isPartialIndex(final IndexMetadata indexMetadata) {
        return IndexModule.DataLocalityType.PARTIAL.name()
            .equals(indexMetadata.getSettings().get(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey()));
    }
    /**
     * Constructs a HotToWarmTieringResponse from the rejected indices map
     *
     * @param rejectedIndices the rejected indices map
     * @return the HotToWarmTieringResponse object
     */
    public static HotToWarmTieringResponse constructToHotToWarmTieringResponse(final Map<Index, String> rejectedIndices) {
        final List<HotToWarmTieringResponse.IndexResult> indicesResult = new LinkedList<>();
        for (Map.Entry<Index, String> rejectedIndex : rejectedIndices.entrySet()) {
            indicesResult.add(new HotToWarmTieringResponse.IndexResult(rejectedIndex.getKey().getName(), rejectedIndex.getValue()));
        }
        return new HotToWarmTieringResponse(true, indicesResult);
    }
}
