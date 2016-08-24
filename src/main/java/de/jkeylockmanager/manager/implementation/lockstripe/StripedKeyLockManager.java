/*
 * Copyright 2009 Marc-Olaf Jaschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.jkeylockmanager.manager.implementation.lockstripe;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import de.jkeylockmanager.contract.Contract;
import de.jkeylockmanager.manager.KeyLockManager;
import de.jkeylockmanager.manager.LockCallback;
import de.jkeylockmanager.manager.ReturnValueLockCallback;

/**
 * Implementation of {@link KeyLockManager}.
 *
 * Maintenance operations are implemented using lock striping.
 *
 * All resources used by one key are freed immediately, if there is no longer a thread in the locked block for this key.
 *
 * @author Marc-Olaf Jaschke
 *
 */
public final class StripedKeyLockManager implements KeyLockManager {


	/**
	 * Default number of Stripes
	 */
	private static final int DEFAULT_NUMBER_OF_STRIPES = 16;
	
	/**
	 * Creates exclusive locks.
	 */
	private static final Function<Object, ReentrantLock> lockCreator = (o) -> new ReentrantLock();


	private final StripedResourceManager<ReentrantLock> resourceManager;
	private final long lockTimeout;
    private final TimeUnit lockTimeoutUnit;


	/**
	 * Creates a new instance of {@link StripedKeyLockManager} with the a default number of stripes
	 *
	 * see #StripedKeyLockManager(long, java.util.concurrent.TimeUnit, int)
	 *
	 */
	public StripedKeyLockManager(final long lockTimeout, final TimeUnit lockTimeoutUnit) {
		this(lockTimeout, lockTimeoutUnit, DEFAULT_NUMBER_OF_STRIPES);
	}

	/**
	 * Creates a new instance of {@link StripedKeyLockManager} with the given settings.
	 *
	 * @param lockTimeout
	 *            the time to wait for a lock before a Exception is thrown - must be greater than 0
	 * @param lockTimeoutUnit
	 *            the unit for lockTimeout - must not be null
	 * @param numberOfStripes
	 *            the number of stripes used for locking
	 */
	public StripedKeyLockManager(final long lockTimeout, final TimeUnit lockTimeoutUnit, final int numberOfStripes) {
		Contract.isNotNull(lockTimeoutUnit, "lockTimeoutUnit != null");
		Contract.isTrue(lockTimeout > 0, "lockTimeout > 0");
		Contract.isTrue(numberOfStripes > 0, "numberOfStripes > 0");
		
		this.resourceManager = new StripedResourceManager<>(lockCreator, lockTimeout, lockTimeoutUnit);
		this.lockTimeout = lockTimeout;
		this.lockTimeoutUnit = lockTimeoutUnit;
	}


	@Override
	public final void executeLocked(final Object key, final LockCallback callback) {
		Contract.isNotNull(key, "key != null");
		Contract.isNotNull(callback, "callback != null");

		executeLockedInternal(key, () -> {
            callback.doInLock();
            return null;
        });

	}

	@Override
	public final <R> R executeLocked(final Object key, final ReturnValueLockCallback<R> callback) {
		Contract.isNotNull(key, "key != null");
		Contract.isNotNull(callback, "callback != null");

		return executeLockedInternal(key, callback);
	}


	private <R> R executeLockedInternal(final Object key, final ReturnValueLockCallback<R> callback) {
		assert key != null : "contract broken: key != null";
		assert callback != null : "contract broken: callback != null";
		
		return this.resourceManager.doWithResource(key, (lock) -> {
			CountingResourceHolder.lockOrThrowException(lock, lockTimeout, lockTimeoutUnit);
			try {
				return callback.doInLock();
			} finally {
				lock.unlock();
			}
		});
		
	}
	
	


	/**
     * for testing only
     *
     * @return the number of currently active key locks
     *
     */
    int activeKeyLocksCount() {
        return this.resourceManager.key2lock.size();
    }
	
	/**
	 * for testing only
	 *
	 * @return the number of threads currently waiting in the queues of the key locks
	 */
	int waitingThreadsCount() {
		int result = 0;
		for (final CountingResourceHolder<ReentrantLock> lock : this.resourceManager.key2lock.values()) {
			result += lock.getLock().getQueueLength();
		}
		return result;
	}
}
