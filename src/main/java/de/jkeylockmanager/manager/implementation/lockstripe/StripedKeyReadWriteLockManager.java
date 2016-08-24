package de.jkeylockmanager.manager.implementation.lockstripe;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import de.jkeylockmanager.contract.Contract;
import de.jkeylockmanager.manager.LockCallback;
import de.jkeylockmanager.manager.ReadWriteKeyLockManager;
import de.jkeylockmanager.manager.ReturnValueLockCallback;

public class StripedKeyReadWriteLockManager implements ReadWriteKeyLockManager {

    /**
     * Default number of Stripes
     */
    private static final int DEFAULT_NUMBER_OF_STRIPES = 16;
    
    /**
     * Creates exclusive locks.
     */
    private static final Function<Object, ReentrantReadWriteLock> lockCreator = 
            (o) -> new ReentrantReadWriteLock();


    private final StripedResourceManager<ReentrantReadWriteLock> resourceManager;


    /**
     * Creates a new instance of {@link StripedKeyLockManager} with the a default number of stripes
     *
     * see #StripedKeyLockManager(long, java.util.concurrent.TimeUnit, int)
     *
     */
    public StripedKeyReadWriteLockManager(final long lockTimeout, final TimeUnit lockTimeoutUnit) {
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
    public StripedKeyReadWriteLockManager(final long lockTimeout, final TimeUnit lockTimeoutUnit, final int numberOfStripes) {
        Contract.isNotNull(lockTimeoutUnit, "lockTimeoutUnit != null");
        Contract.isTrue(lockTimeout > 0, "lockTimeout > 0");
        Contract.isTrue(numberOfStripes > 0, "numberOfStripes > 0");
        
        this.resourceManager = new StripedResourceManager<>(lockCreator, lockTimeout, lockTimeoutUnit);
    }

    @Override
    public void executeWithReadLock(Object key, LockCallback callback) {
        Contract.isNotNull(key, "key != null");
        Contract.isNotNull(callback, "callback != null");

        executeWithReadLockInternal(key, () -> {
            callback.doInLock();
            return null;
        });
    }
    
    @Override
    public <R> R executeWithReadLock(Object key, ReturnValueLockCallback<R> callback) {
        Contract.isNotNull(key, "key != null");
        Contract.isNotNull(callback, "callback != null");

        return executeWithReadLockInternal(key, callback);
    }

    @Override
    public void executeWithWriteLock(Object key, LockCallback callback) {
        Contract.isNotNull(key, "key != null");
        Contract.isNotNull(callback, "callback != null");

        executeWithWriteLockInternal(key, () -> {
            callback.doInLock();
            return null;
        });
    }

    @Override
    public <R> R executeWithWriteLock(Object key, ReturnValueLockCallback<R> callback) {
        Contract.isNotNull(key, "key != null");
        Contract.isNotNull(callback, "callback != null");

        return executeWithWriteLockInternal(key, callback);
    }
    
    private <R> R executeWithReadLockInternal(final Object key, final ReturnValueLockCallback<R> callback) {
        return executeLockedInternal(key, (l) -> l.readLock(), callback);
    }
    
    private <R> R executeWithWriteLockInternal(final Object key, final ReturnValueLockCallback<R> callback) {
        return executeLockedInternal(key, (l) -> l.writeLock(), callback);
    }


    private <R> R executeLockedInternal(final Object key,
        Function<ReentrantReadWriteLock, Lock> toLock,
        final ReturnValueLockCallback<R> callback) {
        assert key != null : "contract broken: key != null";
        assert callback != null : "contract broken: callback != null";
        
        return this.resourceManager.doWithResource(key, (lock) -> {
            
            toLock.apply(lock).lock();
            try {
                return callback.doInLock();
            } finally {
                toLock.apply(lock).unlock();
            }
        });
        
    }
    
}
