package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null || requestType == LockType.NL) return;

        // You may find these variables useful
        LockContext ancestor = lockContext.parent;
        List<LockContext> temp = new ArrayList<>();
        while(ancestor!=null){
            temp.add(ancestor);
            ancestor = ancestor.parent;
        }
        Collections.reverse(temp);
        for(LockContext lc:temp){
            LockType lt = lc.getExplicitLockType(transaction);
            if(!LockType.canBeParentLock(lt,requestType)){
                if(lt!=LockType.NL){
                    if(requestType==LockType.S){
                        assert false;
                    } else {
                        //lt is either IS or S
                        assert lt==LockType.IS || lt == LockType.S;
                        lc.promote(transaction,lt==LockType.IS?LockType.IX:LockType.SIX);
                    }
                } else {
                    lc.acquire(transaction, requestType == LockType.S ? LockType.IS : LockType.IX);
                }
            } else {
                if(requestType == LockType.S){
                    if(lt==LockType.S || lt == LockType.SIX || lt == LockType.X) return;
                } else {
                    if(lt==LockType.X) return;
                }
            }
        }
        LockType lt = lockContext.getExplicitLockType(transaction);
        List<Lock> locks = lockContext.lockman.getLocks(transaction);
        if(!LockType.substitutable(lt,requestType)){
            if(requestType==LockType.S){
                if(lt==LockType.NL){
                    lockContext.acquire(transaction,LockType.S);
                }else if(lt==LockType.IS) {
                    lockContext.escalate(transaction);
                } else if(lt == LockType.IX){
                    lockContext.promote(transaction, LockType.SIX);
                } else {
                    assert false;
                }
            } else {
                if(lt==LockType.NL) {
                    lockContext.acquire(transaction, LockType.X);
                } else {
                    lockContext.promote(transaction,LockType.X);
                }
            }
        }
    }

    // TODO(proj4_part2) add any helper methods you want
}
