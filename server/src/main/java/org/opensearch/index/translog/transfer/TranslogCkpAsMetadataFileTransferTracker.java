/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;

import java.util.HashMap;
import java.util.Set;

/**
 * A subclass of {@link FileTransferTracker} that tracks the transfer state of translog files for generation
 * when translog ckp file is uploaded as translog tlog file metadata
 *
 * @opensearch.internal
 */
public class TranslogCkpAsMetadataFileTransferTracker extends FileTransferTracker {

    public TranslogCkpAsMetadataFileTransferTracker(ShardId shardId, RemoteTranslogTransferTracker remoteTranslogTransferTracker) {
        super(shardId, remoteTranslogTransferTracker);
    }

    public void onSuccess(TranslogCheckpointSnapshot fileSnapshot) {
        try {
            updateUploadTimeInRemoteTranslogTransferTracker();
            updateTranslogTransferStats(fileSnapshot.getTranslogFileName(), true);
            updateTranslogTransferStats(fileSnapshot.getCheckpointFileName(), true);
        } catch (Exception ex) {
            logger.error("Failure to update translog generation upload success stats", ex);
        }
        addGeneration(fileSnapshot.getGeneration(), true);
    }

    public void onFailure(TranslogCheckpointSnapshot fileSnapshot, Exception e) {
        updateUploadTimeInRemoteTranslogTransferTracker();
        updateTranslogTransferStats(fileSnapshot.getTranslogFileName(), false);
        updateTranslogTransferStats(fileSnapshot.getCheckpointFileName(), false);
        addGeneration(fileSnapshot.getGeneration(), false);
    }

    @Override
    public boolean isUploaded(String generation) {
        return super.isGenerationUploaded(Long.parseLong(generation));
    }

    @Override
    public Set<String> allUploaded() {
        return super.allUploadedGeneration();
    }

    @Override
    void recordBytesForFiles(Set<TranslogCheckpointSnapshot> toUpload) {
        bytesForTlogCkpFileToUpload = new HashMap<>();
        toUpload.forEach(file -> {
            recordFileContentLength(file.getTranslogFileName(), file::getTranslogFileContentLength);
            recordFileContentLength(file.getCheckpointFileName(), file::getCheckpointFileContentLength);
        });
    }

    @Override
    void deleteGenerations(Set<Long> generations) {
        for (Long generation : generations) {
            generationTransferTracker.remove(Long.toString(generation));
        }
    }
}