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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.opensearch.core.action.ActionListener;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;

public class TranslogCheckpointSnapshotTransferManagerWithMetadata implements TranslogCheckpointSnapshotTransferManager{

    private final TransferService transferService;

    public TranslogCheckpointSnapshotTransferManagerWithMetadata(TransferService transferService){
        this.transferService = transferService;
    }

    @Override
    public void transferTranslogCheckpointSnapshot(TransferSnapshot transferSnapshot,
                                                   Set<TranslogCheckpointSnapshot> toUpload,
                                                   Map<Long, BlobPath> blobPathMap,
                                                   LatchedActionListener<TranslogCheckpointSnapshot> latchedActionListener,
                                                   WritePriority writePriority) throws Exception {

        for( TranslogCheckpointSnapshot translogCheckpointSnapshot : toUpload) {
            Set<TransferFileSnapshot> filesToUpload = new HashSet<>();
            filesToUpload.add(translogCheckpointSnapshot.getTranslogFileSnapshotWithMetadata());
            ActionListener<TransferFileSnapshot> actionListener = ActionListener.wrap(res -> {
                latchedActionListener.onResponse(translogCheckpointSnapshot);
            }, ex -> {
                latchedActionListener.onFailure(new GenerationTransferException(translogCheckpointSnapshot, ex));
            });

            transferService.uploadBlobs(filesToUpload, blobPathMap, actionListener, WritePriority.HIGH);
        }
    }
}
