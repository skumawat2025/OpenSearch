/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.listener;

import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.TranslogCheckpointSnapshot;

/**
 * The listener to be invoked on the completion or failure of a {@link TransferFileSnapshot} or deletion of file
 *
 * @opensearch.internal
 */
public interface FileTransferListener<T> {

    /**
     * Invoked when the transfer of a single {@link TransferFileSnapshot} succeeds
     * @param fileSnapshot the corresponding file snapshot
     */
    void onSuccess(TranslogCheckpointSnapshot fileSnapshot);

    /**
     * Invoked when the transfer of a single {@link TransferFileSnapshot} fails
     * @param fileSnapshot the corresponding file snapshot
     * @param e the exception while processing the {@link TransferFileSnapshot}
     */
    void onFailure(TranslogCheckpointSnapshot fileSnapshot, Exception e);

}
