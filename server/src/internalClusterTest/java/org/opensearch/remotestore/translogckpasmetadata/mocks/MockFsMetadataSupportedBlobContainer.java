/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.translogckpasmetadata.mocks;

import org.apache.lucene.index.CorruptIndexException;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.slf4j.ILoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MockFsMetadataSupportedBlobContainer extends FsBlobContainer implements AsyncMultiStreamBlobContainer {

    private static final int TRANSFER_TIMEOUT_MILLIS = 30000;

    private final boolean triggerDataIntegrityFailure;

    public MockFsMetadataSupportedBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path, boolean triggerDataIntegrityFailure) {
        super(blobStore, blobPath, path);
        this.triggerDataIntegrityFailure = triggerDataIntegrityFailure;
    }

    @Override
    public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {

        int nParts = 10;
        long partSize = writeContext.getFileSize() / nParts;
        StreamContext streamContext = writeContext.getStreamProvider(partSize);
        final Path file = path.resolve(writeContext.getFileName());
        byte[] buffer = new byte[(int) writeContext.getFileSize()];

        // store .ckp file seperately..
        if(writeContext.getMetadata() != null){
            String base64String = writeContext.getMetadata().get("ckp-data");
            byte[] decodedBytes = Base64.getDecoder().decode(base64String);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(decodedBytes);
            int length = decodedBytes.length;
            writeBlob(getCheckpointFileName(writeContext.getFileName()), inputStream, length, true);
        }

        AtomicLong totalContentRead = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(streamContext.getNumberOfParts());
        for (int partIdx = 0; partIdx < streamContext.getNumberOfParts(); partIdx++) {
            int finalPartIdx = partIdx;
            Thread thread = new Thread(() -> {
                try {
                    InputStreamContainer inputStreamContainer = streamContext.provideStream(finalPartIdx);
                    InputStream inputStream = inputStreamContainer.getInputStream();
                    long remainingContentLength = inputStreamContainer.getContentLength();
                    long offset = partSize * finalPartIdx;
                    while (remainingContentLength > 0) {
                        int readContentLength = inputStream.read(buffer, (int) offset, (int) remainingContentLength);
                        totalContentRead.addAndGet(readContentLength);
                        remainingContentLength -= readContentLength;
                        offset += readContentLength;
                    }
                    inputStream.close();
                } catch (IOException e) {
                    completionListener.onFailure(e);
                } finally {
                    latch.countDown();
                }
            });
            thread.start();
        }
        try {
            if (!latch.await(TRANSFER_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                throw new IOException("Timed out waiting for file transfer to complete for " + writeContext.getFileName());
            }
        } catch (InterruptedException e) {
            throw new IOException("Await interrupted on CountDownLatch, transfer failed for " + writeContext.getFileName());
        }
        try (OutputStream outputStream = Files.newOutputStream(file, StandardOpenOption.CREATE_NEW)) {
            outputStream.write(buffer);
        }
        if (writeContext.getFileSize() != totalContentRead.get()) {
            throw new IOException(
                "Incorrect content length read for file "
                    + writeContext.getFileName()
                    + ", actual file size: "
                    + writeContext.getFileSize()
                    + ", bytes read: "
                    + totalContentRead.get()
            );
        }

        try {
            // bulks need to succeed for segment files to be generated
            if (isSegmentFile(writeContext.getFileName()) && triggerDataIntegrityFailure) {
                completionListener.onFailure(
                    new RuntimeException(
                        new CorruptIndexException(
                            "Data integrity check failure for file: " + writeContext.getFileName(),
                            writeContext.getFileName()
                        )
                    )
                );
            } else {
                writeContext.getUploadFinalizer().accept(true);
                completionListener.onResponse(null);
            }
        } catch (Exception e) {
            completionListener.onFailure(e);
        }

    }


    private String getCheckpointFileName(String translogFileName) {
        if (!translogFileName.endsWith(".tlog")) {
            throw new IllegalArgumentException("Invalid translog file name format: " + translogFileName);
        }

        int dotIndex = translogFileName.lastIndexOf('.');
        String baseName = translogFileName.substring(0, dotIndex);
        return baseName + ".ckp";
    }

    public static String convertToBase64(InputStream inputStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[1024];
        int nRead;

        while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }

        buffer.flush();
        byte[] byteArray = buffer.toByteArray();
        String base64String = Base64.getEncoder().encodeToString(byteArray);

        buffer.close();
        inputStream.close();

        return base64String;
    }

    @Override
    public InputStreamWithMetadata readBlobWithMetadata(String blobName) throws IOException {
        InputStream inputStream = readBlob(blobName);
        InputStream ckpInputStream = readBlob(getCheckpointFileName(blobName));
        String ckpString = convertToBase64(ckpInputStream);
        Map<String, String> metadata = new HashMap<>();
        metadata.put("ckp-data", ckpString);
        return new InputStreamWithMetadata(inputStream, metadata);
    }

    @Override
    public void readBlobAsync(String blobName, ActionListener<ReadContext> listener) {
        new Thread(() -> {
            try {
                long contentLength = listBlobs().get(blobName).length();
                long partSize = contentLength / 10;
                int numberOfParts = (int) ((contentLength % partSize) == 0 ? contentLength / partSize : (contentLength / partSize) + 1);
                List<ReadContext.StreamPartCreator> blobPartStreams = new ArrayList<>();
                for (int partNumber = 0; partNumber < numberOfParts; partNumber++) {
                    long offset = partNumber * partSize;
                    InputStreamContainer blobPartStream = new InputStreamContainer(readBlob(blobName, offset, partSize), partSize, offset);
                    blobPartStreams.add(() -> CompletableFuture.completedFuture(blobPartStream));
                }
                ReadContext blobReadContext = new ReadContext.Builder(contentLength, blobPartStreams).build();
                listener.onResponse(blobReadContext);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }).start();
    }

    public boolean remoteIntegrityCheckSupported() {
        return true;
    }

    private boolean isSegmentFile(String filename) {
        return !filename.endsWith(".tlog") && !filename.endsWith(".ckp");
    }
}
