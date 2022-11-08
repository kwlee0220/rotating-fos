/*
 * Copyright 2018-2022 Volkan Yazıcı
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permits and
 * limitations under the License.
 */

package com.vlkan.rfos;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vlkan.rfos.policy.DailyRotationPolicy;
import com.vlkan.rfos.policy.RotationPolicy;
import com.vlkan.rfos.policy.SizeBasedRotationPolicy;
import com.vlkan.rfos.policy.WeeklyRotationPolicy;

import jarvey.FilePath;

import utils.LoggerSettable;

/**
 * A thread-safe {@link OutputStream} targeting a file where rotation of the
 * active stream is supported.
 * <p>
 * Rotation can be triggered by either manually using
 * {@link #rotate(RotationPolicy, Instant)} method or indirectly using the
 * registered {@link RotationPolicy} set.
 * </p><p>
 * Interception of state changes are supported by the registered
 * {@link RotationCallback} set.
 * </p>
 *
 * @see LoggingRotationCallback
 * @see DailyRotationPolicy
 * @see WeeklyRotationPolicy
 * @see SizeBasedRotationPolicy
 */
public class RotatingFileOutputStream extends OutputStream implements Rotatable, LoggerSettable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RotatingFileOutputStream.class);

    protected final FilePath m_file;
    protected final RotationConfig m_config;
    private final List<RotationCallback> callbacks;
    private final List<RotationPolicy> writeSensitivePolicies;
    private volatile ByteCountingOutputStream stream;
    private volatile Logger m_logger = LOGGER;

    /**
     * Constructs an instance using the given configuration
     *
     * @param config a configuration instance
     */
    public RotatingFileOutputStream(FilePath file, RotationConfig config) {
    	m_file = file;
        this.m_config = Objects.requireNonNull(config, "config");
        this.callbacks = new ArrayList<>(config.getCallbacks());
        this.writeSensitivePolicies = collectWriteSensitivePolicies(config.getPolicies());
        this.stream = open(null, config.getClock().now());
        startPolicies();
        
        setLogger(LOGGER);
    }

    private static List<RotationPolicy> collectWriteSensitivePolicies(Set<RotationPolicy> policies) {
        List<RotationPolicy> writeSensitivePolicies = new ArrayList<>();
        for (RotationPolicy policy : policies) {
            if (policy.isWriteSensitive()) {
                writeSensitivePolicies.add(policy);
            }
        }
        return writeSensitivePolicies;
    }

    private void startPolicies() {
        for (RotationPolicy policy : m_config.getPolicies()) {
            policy.start(this);
        }
    }

    protected ByteCountingOutputStream open(RotationPolicy policy, Instant instant) {
		try {
			OutputStream os = m_config.isAppend() ? m_file.append() : m_file.create(true);
			invokeCallbacks(callback -> callback.onOpen(policy, instant, os));
			long size = m_config.isAppend() ? m_file.getLength() : 0;
			return new ByteCountingOutputStream(os, size);
		}
		catch ( IOException error ) {
			String message = String.format("file open failure {file=%s}", m_file);
			throw new RuntimeException(message, error);
		}
    }
    
    public FilePath getFile() {
    	return m_file;
    }

    @Override
    public void rotate(RotationPolicy policy, Instant instant) {
        try {
            unsafeRotate(policy, instant);
        } catch (Exception error) {
            String message = String.format("rotation failure {instant=%s}", instant);
            RuntimeException extendedError = new RuntimeException(message, error);
            invokeCallbacks(callback -> callback.onFailure(policy, instant, null, extendedError));
        }
    }

    private synchronized void unsafeRotate(RotationPolicy policy, Instant instant) throws Exception {
		// Check arguments.
		Objects.requireNonNull(instant, "instant");

		// Check the state.
		unsafeCheckStream();

		// Notify the trigger listeners.
		invokeCallbacks(callback -> callback.onTrigger(policy, instant));

		// Skip rotation if the file is empty.
		stream.flush();

		// Close the file. (Required before rename on Windows!, before get-length on HDFS!)
		invokeCallbacks(callback -> callback.onClose(policy, instant, stream));
		stream.close();
		
		if ( m_file.getLength() > 0 ) {
			// Backup file, if enabled.
			FilePath rotatedFile;
			if ( m_config.getMaxBackupCount() > 0 ) {
				renameBackups();
				rotatedFile = backupFile();
			}

			// Otherwise, rename using the provided file pattern.
			else {
				String rotatedFilePath = m_config.getFilePattern().create(instant);
				rotatedFile = m_file.path(rotatedFilePath);
				getLogger().debug("renaming {file={}, ro tatedFile={}}", m_file, rotatedFile);
				m_file.renameTo(rotatedFile, true);
			}
			
			// Re-open the file.
 			getLogger().debug("re-opening file {file={}}", m_file);
			stream = open(policy, instant);

			// Compress the old file, if necessary.
			if ( m_config.isCompress() ) {
				asyncCompress(policy, instant, rotatedFile);
				return;
			}

			// So far, so good;
			invokeCallbacks(callback -> callback.onSuccess(policy, instant, rotatedFile));
		}
		else {
			getLogger().info("empty file, skipping rotation: file={}", m_file);
			m_file.delete();
			
			// Re-open the file.
			getLogger().debug("re-opening file {file={}}", m_file);
			stream = open(policy, instant);
		}
    }

    private void renameBackups() throws IOException {
    	FilePath dstFile = getBackupFile(m_config.getMaxBackupCount() - 1);
		for ( int backupIndex = m_config.getMaxBackupCount() - 2; backupIndex >= 0; backupIndex-- ) {
			FilePath srcFile = getBackupFile(backupIndex);
			if ( srcFile.exists() ) {
				getLogger().debug("renaming backup {srcFile={}, dstFile={}}", srcFile, dstFile);
				srcFile.renameTo(dstFile, true);
			}
			dstFile = srcFile;
		}
    }

    private FilePath backupFile() throws IOException {
    	FilePath dstFile = getBackupFile(0);
        getLogger().debug("renaming for backup {srcFile={}, dstFile={}}", m_file, dstFile);
        m_file.renameTo(dstFile, true);
        return dstFile;
    }

    private static void renameFile(File srcFile, File dstFile) throws IOException {
        Files.move(
                srcFile.toPath(),
                dstFile.toPath(),
                StandardCopyOption.REPLACE_EXISTING/*,      // The rest of the arguments (atomic & copy-attr) are pretty
                StandardCopyOption.ATOMIC_MOVE,             // much platform-dependent and JVM throws an "unsupported
                StandardCopyOption.COPY_ATTRIBUTES*/);      // option" exception at runtime.
    }

    private FilePath getBackupFile(int backupIndex) {
		String backupFileName = m_file.getName() + '.' + backupIndex;
		FilePath parent = m_file.getParent();
        if ( parent != null ) {
        	return parent.getChild(backupFileName);
        }
        else {
        	return m_file.path(backupFileName);
        }
    }

    private void asyncCompress(RotationPolicy policy, Instant instant, FilePath rotatedFile) {
		m_config.getExecutorService().execute(new Runnable() {
			private final String displayName
				= String.format("%s.compress(%s)", getClass().getSimpleName(), rotatedFile);

			@Override
			public void run() {
				FilePath compressedFile = getCompressedFile(rotatedFile);
				try {
					unsafeSyncCompress(rotatedFile, compressedFile);
					invokeCallbacks(callback -> callback.onSuccess(policy, instant, compressedFile));
				}
				catch ( Exception error ) {
					String message = String.format(
							"compression failure {instant=%s, rotatedFile=%s, compressedFile=%s}", instant, rotatedFile,
							compressedFile);
					RuntimeException extendedError = new RuntimeException(message, error);
					invokeCallbacks(callback -> callback.onFailure(policy, instant, rotatedFile, extendedError));
				}
			}

			@Override
			public String toString() {
				return displayName;
			}
		});
    }

    private FilePath getCompressedFile(FilePath rotatedFile) {
        String compressedFileName = String.format("%s.gz", rotatedFile.getAbsolutePath());
        return m_file.path(compressedFileName);
    }

    private static void unsafeSyncCompress(FilePath rotatedFile, FilePath compressedFile) throws IOException {
		// Compress the file.
		LOGGER.debug("compressing {rotatedFile={}, compressedFile={}}", rotatedFile, compressedFile);
		try ( InputStream sourceStream = rotatedFile.read() ) {
			try ( OutputStream targetStream = compressedFile.create(true); 
					GZIPOutputStream gzipTargetStream = new GZIPOutputStream(targetStream) ) {
				copy(sourceStream, gzipTargetStream);
			}
		}

		// Delete the rotated file. (On Windows, delete must take place after closing
		// the file input stream!)
		LOGGER.debug("deleting old file {rotatedFile={}}", rotatedFile);
		rotatedFile.delete();
    }

    private static void copy(InputStream source, OutputStream target) throws IOException {
        byte[] buffer = new byte[8192];
        int readByteCount;
        while ((readByteCount = source.read(buffer)) > 0) {
            target.write(buffer, 0, readByteCount);
        }
    }

    @Override
    public RotationConfig getConfig() {
        return m_config;
    }

    @Override
    public synchronized void write(int b) throws IOException {
        unsafeCheckStream();
        long byteCount = stream.size() + 1;
        notifyWriteSensitivePolicies(byteCount);
        stream.write(b);
    }

    @Override
    public synchronized void write(byte[] b) throws IOException {
        unsafeCheckStream();
        long byteCount = stream.size() + b.length;
        notifyWriteSensitivePolicies(byteCount);
        stream.write(b);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        unsafeCheckStream();
        long byteCount = stream.size() + len;
        notifyWriteSensitivePolicies(byteCount);
        stream.write(b, off, len);
    }

    private void notifyWriteSensitivePolicies(long byteCount) {
        // noinspection ForLoopReplaceableByForEach (avoid iterator instantion)
        for (int writeSensitivePolicyIndex = 0;
             writeSensitivePolicyIndex < writeSensitivePolicies.size();
             writeSensitivePolicyIndex++) {
            RotationPolicy writeSensitivePolicy = writeSensitivePolicies.get(writeSensitivePolicyIndex);
            writeSensitivePolicy.acceptWrite(byteCount);
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (stream != null) {
            stream.flush();
        }
    }

    /**
     * Unless the stream is already closed, invokes registered callbacks,
     * stops registered policies, and closes the active stream.
     */
    @Override
    public synchronized void close() throws IOException {
        if (stream == null) {
            return;
        }
        invokeCallbacks(callback -> callback.onClose(null, m_config.getClock().now(), stream));
        stopPolicies();
        stream.close();
        stream = null;
    }

    private void stopPolicies() {
        m_config.getPolicies().forEach(RotationPolicy::stop);
    }

    protected void invokeCallbacks(Consumer<RotationCallback> invoker) {
        // noinspection ForLoopReplaceableByForEach (avoid iterator instantion)
        for (int callbackIndex = 0; callbackIndex < callbacks.size(); callbackIndex++) {
            RotationCallback callback = callbacks.get(callbackIndex);
            invoker.accept(callback);
        }
    }

    private void unsafeCheckStream() throws IOException {
        if (stream == null) {
            throw new IOException("either closed or not initialized yet");
        }
    }

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = (logger != null) ? logger : LOGGER;
	}

    @Override
    public String toString() {
        return String.format("RotatingFileOutputStream{file=%s}", m_file);
    }
}
