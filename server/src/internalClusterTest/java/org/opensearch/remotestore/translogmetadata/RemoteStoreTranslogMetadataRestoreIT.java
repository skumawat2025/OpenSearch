/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.translogmetadata;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreRestoreIT;
import org.opensearch.remotestore.translogmetadata.mocks.MockFsMetadataSupportedRepositoryPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreTranslogMetadataRestoreIT extends RemoteStoreRestoreIT {
    Path repositoryLocation;
    boolean compress;
    boolean overrideBuildRepositoryMetadata;

    @Before
    public void setup() {
        clusterSettingsSuppliedByTest = true;
        overrideBuildRepositoryMetadata = false;
        repositoryLocation = randomRepoPath();
        compress = randomBoolean();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockFsMetadataSupportedRepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(
                remoteStoreClusterSettings(
                    REPOSITORY_NAME,
                    segmentRepoPath,
                    MockFsMetadataSupportedRepositoryPlugin.TYPE_MD,
                    REPOSITORY_2_NAME,
                    translogRepoPath,
                    MockFsMetadataSupportedRepositoryPlugin.TYPE_MD
                )
            )
            .build();
    }
}
