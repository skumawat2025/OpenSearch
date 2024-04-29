/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;

import java.util.Map;
import java.util.Set;

public interface TranslogCheckpointSnapshotTransferManager {

    public void transferTranslogCheckpointSnapshot(TransferSnapshot transferSnapshot,
                                                   Set<TranslogCheckpointSnapshot> toUpload,
                                                   Map<Long, BlobPath> blobPathMap,
                                                   LatchedActionListener<FileSnapshot.TransferFileSnapshot> latchedActionListener,
                                                   WritePriority writePriority) throws Exception;
}
