/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.node.Node;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.getRemoteStoreTranslogRepo;

/**
 * Determines the {@link RemoteStorePathStrategy} at the time of index metadata creation.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class RemoteStoreCustomMetadataResolver {

    public final static String REMOTE_CUSTOM_METADATA_ATTRIBUTE_KEY = "optimised_remote_store_enable";
    public final static String REMOTE_CUSTOM_METADATA_NODE_ATTR_KEY = Node.NODE_ATTRIBUTES.getKey() + REMOTE_CUSTOM_METADATA_ATTRIBUTE_KEY;

    private final RemoteStoreSettings remoteStoreSettings;
    private final ClusterService clusterService;
    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final Settings settings;

    public RemoteStoreCustomMetadataResolver(
        RemoteStoreSettings remoteStoreSettings,
        ClusterService clusterService,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Settings settings
    ) {
        this.remoteStoreSettings = remoteStoreSettings;
        this.clusterService = Objects.requireNonNull(clusterService);
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.settings = settings;
    }

    public RemoteStorePathStrategy getPathStrategy() {
        PathType pathType;
        PathHashAlgorithm pathHashAlgorithm;
        // Min node version check ensures that we are enabling the new prefix type only when all the nodes understand it.
        pathType = isRemoteCustomMetadataEnabled() ? remoteStoreSettings.getPathType() : PathType.FIXED;
        // If the path type is fixed, hash algorithm is not applicable.
        pathHashAlgorithm = pathType == PathType.FIXED ? null : remoteStoreSettings.getPathHashAlgorithm();
        return new RemoteStorePathStrategy(pathType, pathHashAlgorithm);
    }

    private boolean isRemoteCustomMetadataEnabled() {
        Map<String, DiscoveryNode> nodesMap = Collections.unmodifiableMap(clusterService.state().nodes().getNodes());

        if (nodesMap.isEmpty()) {
            return false;
        }

        for (String node : nodesMap.keySet()) {
            DiscoveryNode nodeDiscovery = nodesMap.get(node);
            Map<String, String> nodeAttributes = nodeDiscovery.getAttributes();
            if (!nodeAttributes.containsKey(REMOTE_CUSTOM_METADATA_ATTRIBUTE_KEY)) {
                return false;
            }
        }
        return true;
    }

    public boolean isTranslogMetadataEnabled() {
        Repository repository;
        try {
            repository = repositoriesServiceSupplier.get().repository(getRemoteStoreTranslogRepo(settings));
        } catch (RepositoryMissingException ex) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", ex);
        }
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        return isRemoteCustomMetadataEnabled()
            && remoteStoreSettings.isTranslogMetadataEnabled()
            && blobStoreRepository.blobStore().isBlobMetadataEnabled();
    }

}
