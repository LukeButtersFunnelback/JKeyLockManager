package de.jkeylockmanager.manager;


public interface ReadWriteKeyLockManager {

    
    void executeWithReadLock(Object key, LockCallback callback);
    
    void executeWithWriteLock(Object key, LockCallback callback);

    <R> R executeWithReadLock(Object key, ReturnValueLockCallback<R> callback);
    
    <R> R executeWithWriteLock(Object key, ReturnValueLockCallback<R> callback);
}
