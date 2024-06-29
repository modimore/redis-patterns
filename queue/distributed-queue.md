# Distributed Queue

A distributed queue provides a way to share work among multiple agents. This can mean both creating work by adding items to a queue and performing work after claiming items from a queue.

A distributed queue can be written just like a normal queue, to be interacted with using the expected push and pop operations. In a distributed context however this can be too limiting because an agent could pop and item from the queue and fail to process it fully, leaving work not completed while another agent would be able to complete it. It might be acceptable to lose work in this way, but assuming it's not we may have to expand our model of a queue.

For our distributed queue we will define three operations: push, take, and complete. Pushing to the queue adds an item to be processed and consumed from the queue. The operations take and complete together form a two-step equivalent to popping from a queue, where take finds and item on the queue for processing but without fully removing it and complete accepts an item that has been taken previously and removes it from the queue. A consumer of the queue should call take, process the queue item, and then call complete with the item that was taken.

Here two variants of a distributed queue made with Redis are presented. The first is a queue implemented as a list of pending items paired with a list of in-progress items, which can serve as a backup. The second is a single list of items used as a circular buffer where items are expected to be locked when claimed to limit redundant processing.

Note that there are a number of other pieces of software that can serve as a distributed queue, many of which are specifically designed for that. Redis can be a good choice, especially for an ad-hoc use-case or a proof-of-concept, but there are definitely other options to consider for the general problem of having a distributed queue.

## Queue with In-Progress Backup

This is a queue that stores its pending work in a list of in-progress items, and when an item is picked up by a consumer moves that item to a list of in-progress items. This is to prevent total loss of work when an consumer crashes or fails to do the work. In this situation technically the work is recoverable because it still exists on the in-progress backup list. The lists in question in this case are Redis lists, which have commands that are convenient to modeling queue operations.

The plan to recover items in the case of a processing failure is a detail not covered here. One option that may be reasonable is expecting there to be some monitoring of the in-progress work and having a developer manually shovel items that are nominally in-progress back onto the pending work queue or discard them based on their judgement. If failures are rare enough or dealing with them can usually wait this can be a managable solution. Automated recovery is a more complex topic and there isn't one good generic solution for how to restore the backed-up items.

### Operations

#### Push

Push an item onto the pending queue. The `LPUSH` command can be used for this.

#### Claim

Remove an item from the pending queue and place it on the in-progress backup list. The `RPOPLPUSH` command can be used for this. The item should be processed after being claimed.

#### Complete

Remove an item from the in-progress backup after processing. The `LREM` command can be used for this.

## Circular Buffer with Item Locking

This queue uses a list as a circular buffer for pending work. Items are added at the end of the queue. When a consumer is ready to pick up an item it moves it from the front of the queue to the back of the queue and attempts to acquire a lock for that item. If the lock is acquired that item can be processed, and if not that indicates that another consumer is processing the item. The lock should have an expiration set to a reasonable duration of time in which the item could be processed.

In this scheme an item that has never been completed is always on the queue and visible to all consumers, but there is a clear condition on when it is available to claim. Recovery in the case of a processing failure is automatic because the item can be claimed by any consumer because it is still on the queue when the lock expires.

### Operations

#### Push

Push an item onto the queue buffer. The `LPUSH` command can be used for this.

#### Claim

Pop an item from the front of the queue buffer and replace it at the back of the queue buffer. Attempt to lock the item for processing. If the lock is acquired, return the item to the consumer for processing. If the lock cannot be acquired another consumer has claimed the item for processing so return that no item was retrieved.

This takes two commands, something to move the item and something to create the lock for the item. `RPOPLPUSH` can be used to move the item and `SET` to create the lock.

#### Complete

Remove the item from the queue buffer and release the lock on the item. This takes two commands, and can be done with `LREM` to remove the item and `DEL` to release the lock.
