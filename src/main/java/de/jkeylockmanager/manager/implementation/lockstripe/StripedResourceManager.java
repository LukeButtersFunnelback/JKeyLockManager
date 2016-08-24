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

import static java.lang.Math.abs;
import static java.util.Arrays.setAll;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import de.jkeylockmanager.contract.Contract;

/**
 * Managers lock resources.
 * 
 * <p>This is responsible for keeping used locks and removing unused locks.</p>
 * 
 * <p>Note that the given lockTimeout is also used on the internal striped locks. I don't
 * think this makes sense as it could lead to inconsistency, we should wait forever on the
 * striped lock. TODO tell the project guys about this.</p>
 * 
 * @author Marc-Olaf Jaschke
 *
 * @param <T> The type of lock this class is responsible for manahing
 */
public final class StripedResourceManager<T> {


	/**
	 * Default number of Stripes
	 */
	private static final int DEFAULT_NUMBER_OF_STRIPES = 16;


	final ConcurrentHashMap<Object, CountingResourceHolder<T>> key2lock = new ConcurrentHashMap<>();
	
	private final CountingResourceHolder<Lock>[] stripes;
	private final long lockTimeout;
	private final TimeUnit lockTimeoutUnit;
	
	private final Function<Object, T> createLockForKey;


	/**
	 * Creates a new instance of {@link StripedResourceManager} with the a default number of stripes
	 *
	 * see #StripedKeyLockManager(long, java.util.concurrent.TimeUnit, int)
	 *
	 */
	public StripedResourceManager(final Function<Object, T> createLockForKey,
	        final long lockTimeout, 
	        final TimeUnit lockTimeoutUnit) {
		this(createLockForKey, lockTimeout, lockTimeoutUnit, DEFAULT_NUMBER_OF_STRIPES);
	}

	/**
	 * Creates a new instance of {@link StripedResourceManager} with the given settings.
	 *
	 * @param lockTimeout
	 *            the time to wait for a lock before a Exception is thrown - must be greater than 0
	 * @param lockTimeoutUnit
	 *            the unit for lockTimeout - must not be null
	 * @param numberOfStripes
	 *            the number of stripes used for locking
	 */
	public StripedResourceManager(final Function<Object, T> createLockForKey,
	                                final long lockTimeout, 
	                                final TimeUnit lockTimeoutUnit, 
	                                final int numberOfStripes) {
		Contract.isNotNull(lockTimeoutUnit, "lockTimeoutUnit != null");
		Contract.isTrue(lockTimeout > 0, "lockTimeout > 0");
		Contract.isTrue(numberOfStripes > 0, "numberOfStripes > 0");

		this.createLockForKey = createLockForKey;
		this.lockTimeout = lockTimeout;
		this.lockTimeoutUnit = lockTimeoutUnit;
		this.stripes = new CountingResourceHolder[numberOfStripes];

		setAll(stripes, i -> new CountingResourceHolder<Lock>(new ReentrantLock()));
	}

	/**
	 * Lets the caller use a resource, without needint to worry about getting it or putting it back.
	 * 
	 * @param key
	 * @param withResource A function which is given a lock of type T, and is responsible for
	 * takeing and releasing that lock.
	 * @return The result of doWothLock
	 */
	<R> R doWithResource(final Object key, 
	    Function<T, R> withResource) {
		assert key != null : "contract broken: key != null";
		assert withResource != null : "contract broken: callback != null";

		final CountingResourceHolder<T> lock = getKeyLock(key);
		try {
			return withResource.apply(lock.getLock());
		} finally {
			freeKeyLock(key, lock);
		}
	}

	private void freeKeyLock(final Object key, final CountingResourceHolder<T> lock) {
		assert key != null : "contract broken: key != null";
		assert lock != null : "contract broken: lock != null";
		Lock stripedLock = getStripedLock(key).getLock(); 
		CountingResourceHolder.lockOrThrowException(stripedLock, lockTimeout, lockTimeoutUnit);
		try {
			lock.decrementUses();
			if (!lock.isUsed()) {
				key2lock.remove(key);
			}
		} finally {
			stripedLock.unlock();
		}
	}

	private CountingResourceHolder<T> getKeyLock(final Object key) {
		assert key != null : "contract broken: key != null";
		Lock stripedLock = getStripedLock(key).getLock(); 
		CountingResourceHolder.lockOrThrowException(stripedLock, lockTimeout, lockTimeoutUnit);
		try {
			final CountingResourceHolder<T> result;
			final CountingResourceHolder<T> previousLock = key2lock.get(key);
			if (previousLock == null) {
				result = new CountingResourceHolder<T>(this.createLockForKey.apply(key));
				key2lock.put(key, result);
			} else {
				result = previousLock;
			}
			result.incrementUses();
			return result;
		} finally {
			stripedLock.unlock();
		}
	}

	private CountingResourceHolder<Lock> getStripedLock(final Object key) {
		assert key != null : "contract broken: key != null";
		return stripes[abs(key.hashCode() % stripes.length)];
	}
}
