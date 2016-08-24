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

import de.jkeylockmanager.manager.exception.KeyLockManagerInterruptedException;
import de.jkeylockmanager.manager.exception.KeyLockManagerTimeoutException;

/**
 * {@link CountingResourceHolder} adds a counter for counting resource uses.
 * 
 * The counting functionality is not thread safe and so it is essential to use
 * the following methods only in the scope of a shared lock:
 * 
 * {@link #decrementUses()}, {@link #incrementUses()}, {@link #isUsed()}
 * 
 * 
 * @see ReentrantLock
 * 
 * @author Marc-Olaf Jaschke
 * 
 */
final class CountingResourceHolder<T> {

	private final T lock;
	private long uses = 0;

	/**
	 * Creates a new instance of {@link CountingResourceHolder} with a usage counter set
	 * to zero.
	 * 
	 * 
	 */
	CountingResourceHolder(T lock) {
		this.lock = lock;
	}
	
	T getLock() {
        return lock;
	    
	}

	/**
	 * Decrements the usage counter. See class commentary for thread safety!
	 */
	void decrementUses() {
		uses--;
	}

	/**
	 * Increments the usage counter. See class commentary for thread safety!
	 */
	void incrementUses() {
		uses++;
	}

	/**
	 * See class commentary for thread safety!
	 * 
	 * @return true, if the usage counter is zero
	 */
	boolean isUsed() {
		return uses != 0;
	}

	/**
	 * Tries to acquire the lock or throws an exception when the lock is not acquired in time.
	 * 
	 * @throws KeyLockManagerInterruptedException
	 *             if the current thread becomes interrupted while waiting for
	 *             the lock
	 * @throws KeyLockManagerTimeoutException
	 *             if the instance wide waiting time is exceeded
	 */
	static void lockOrThrowException(Lock l, long lockTimeout, TimeUnit lockTimeoutUnit) {
		try {
			if (!l.tryLock(lockTimeout, lockTimeoutUnit)) {
				throw new KeyLockManagerTimeoutException(lockTimeout, lockTimeoutUnit);
			}
		} catch (final InterruptedException e) {
			throw new KeyLockManagerInterruptedException();
		}
	}

}
