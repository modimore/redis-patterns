# Distributed Lock

A lock is a resource that is acquired by an agent before performing a task. If the lock cannot be acquired the task will not be performed. The most-commonly discussed locks are mutually exclusive locks, which can only acquired by a single agent at once and released in order for other agents to acquire it. Between when a lock is successfully acquired and when it is released we consider the lock to be held. The use of a lock prevents competing processes from acting simultaneously. The purpose of adding a lock in a distributed system is generally either achieving correctness or reducing redundancy. This is not strictly different from why a lock managed locally is used.

A distributed lock differs from a local lock in that a lock needs an identifier or name that is well-known among the processes that use it. The process also need to agree on the details of how to use the lock. It is often desirable to have a distributed lock automatically expire after a period of time in case the holder fails processing ungracefully, including crashing with the lock held. Some use the term lease to refer to this kind of lock with expiration. A lock should expire after a period of time that is larger than it is expected to take the work protected by the lock to be completed.

The core operations of a distributed lock are conceptually both an atomic check-and-set mechanism. The acquisition of the lock checks if the lock is currently held and if not acquires the lock on behalf of the requestor. The release of the lock checks if the lock is held by the requestor and if so releases it.

Many lock implementations also support checking if the lock is held. This may seem convenient but it can usually be avoided and if a process doesn't have the lock what it usually wants is just to acquire it. If there is a corresponding and reliable release of the lock for every acquire of the lock then this is generally unnecessary.

In some cases it can be useful to use a lock in a way where it is continuously held to enforce long-standing ownership of a process or resource. The lock should still expire though in case the holder becomes unavailable. In this case a mechanism can be added to the lock to extend ownership for the current holder. This can be useful for things that happen on a schedule and function effectively as distributed rate limiter. For this usage of a lock in particular techniques such as job scheduling or deploying an application on only one server may be better approaches.

## Simple Redis Lock

A distributed lock can easily be implemented on top of Redis. There are two relatively easy ways to perform an atomic check-and-set, using transactions and using Lua scripts. The acquire operation can actually be supported by Redis with a single command. Redis also conviently has a built-in key expiration mechanism, so expiration is simple to add to a Redis-based lock.

With this design each lock instance the key will be a well-known name common to all processes that may acquire the lock and the value should be specific to the acquirer.

This simple lock is easily supported by a Redis deployment of a single server or a cluster with a single primary node. In fact a distributed lock does not benefit from replicas and they may compromise the correctness of the lock.

### Operations

#### Acquire

Set the lock's key to the caller's distinct value if it is not set.

#### Release

Check if the value of the key contains the caller's distinct value and if so delete the key.

#### Check

Check if the value of the key matches the caller's distinct value.

#### Refresh

Check if the value of the key matches the caller's distinct value and if so update the expiry time of the key.
