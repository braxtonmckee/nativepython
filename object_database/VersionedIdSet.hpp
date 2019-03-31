#pragma once

#include <map>
#include <vector>
#include <set>
#include "Common.hpp"
#include <iostream>

/*************

VersionedIdSet stores a collection of objectids (int64_t) that are versioned
by a transaction id.

The set is considered initially empty, and at each transaction ID,
we can add or remove some object ids for a particular value.

For every transactionId, we want to be able to efficiently determine
the number of objects in the set and walk over them.

*************/

class VersionedIdSet {
public:
    VersionedIdSet() :
            mGuaranteedLowestId(NO_TRANSACTION)
    {
    }

    bool empty() const {
        return mObjToTrans.size() == 0 && mPresentAtLowestId.size() == 0;
    }

    transaction_id getGuaranteedLowestId() const {
        return mGuaranteedLowestId;
    }

    transaction_id nextTransactionToMoveForwardOn() const {
        if (!mTransToObj.size()) {
            return NO_TRANSACTION;
        }

        return mTransToObj.begin()->first;
    }

    transaction_id moveGuaranteedLowestIdForward(transaction_id t) {
        if (t < mGuaranteedLowestId) {
            throw std::runtime_error("Can't ask about a transaction id before the lowest guaranteed id");
        }
        if (t == mGuaranteedLowestId) {
            return nextTransactionToMoveForwardOn();
        }

        mGuaranteedLowestId = t;

        while (mTransToObj.size() && mTransToObj.begin()->first <= mGuaranteedLowestId) {
            transaction_id transToDrop = mTransToObj.begin()->first;

            auto& addsAndRemoves = mTransToObj.begin()->second;

            for (auto objAndAdded: addsAndRemoves) {
                if (objAndAdded.second) {
                    mPresentAtLowestId.insert(objAndAdded.first);
                } else {
                    mPresentAtLowestId.erase(objAndAdded.first);
                }

                auto& transForThisObj = mObjToTrans[objAndAdded.first];

                transForThisObj.erase(transToDrop);

                if (transForThisObj.size() == 0) {
                    mObjToTrans.erase(objAndAdded.first);
                }
            }

            mTransToObj.erase(mTransToObj.begin());
        }

        return nextTransactionToMoveForwardOn();
    }


    bool isActive(transaction_id t, object_id o) const {
        if (t < mGuaranteedLowestId) {
            throw std::runtime_error("Can't ask about a transaction id before the lowest guaranteed id");
        }

        auto it = mObjToTrans.find(o);
        if (it == mObjToTrans.end()) {
            return mPresentAtLowestId.find(o) != mPresentAtLowestId.end();
        }

        if (it->second.size() == 0) {
            throw std::runtime_error(
                "Empty transaction lookup table should have been deleted."
                );
        }

        auto t_it = it->second.lower_bound(t);

        //we're at the end. last transaction add/remove dictates
        if (t_it == it->second.end()) {
            t_it--;
            return t_it->second;
        }

        if (t_it->first == t) {
            return t_it->second;
        }

        if (t_it == it->second.begin()) {
            return mPresentAtLowestId.find(o) != mPresentAtLowestId.end();
        }

        t_it--;

        return t_it->second;
    }

    object_id lookupOne(transaction_id t) const {
        object_id o = lookupFirst(t);

        if (o == NO_OBJECT) {
            return NO_OBJECT;
        }

        if (lookupNext(t, o) != NO_OBJECT) {
            return NO_OBJECT;
        }

        return o;
    }

    /******
    find the object_id at a particular transaction id.
    *****/
    object_id lookupFirst(transaction_id t) const {
        return lookupNext(t, NO_OBJECT);
    }

    /******
    find the next object active at this transaction.

    it this could get slow if you have had many objects in the index
    over time but most of them are not active. it would be better to store
    a 'next object' table (that's also versioned).
    ******/
    object_id lookupNext(transaction_id t, object_id o) const {
        auto g_it = mPresentAtLowestId.upper_bound(o);
        auto o_it = mObjToTrans.upper_bound(o);

        //step through both sets at the same time checking each value
        while (g_it != mPresentAtLowestId.end() || o_it != mObjToTrans.end()) {
            if (g_it == mPresentAtLowestId.end()) {
                if (isActive(t, o_it->first)) {
                    return o_it->first;
                } else {
                    o_it++;
                }
            } else if (o_it == mObjToTrans.end()) {
                if (isActive(t, *g_it)) {
                    return *g_it;
                } else {
                    g_it++;
                }
            } else {
                //both are active
                if (o_it->first < *g_it) {
                    if (isActive(t, o_it->first)) {
                        return o_it->first;
                    } else {
                        o_it++;
                    }
                } else if (*g_it < o_it->first) {
                    if (isActive(t, *g_it)) {
                        return *g_it;
                    } else {
                        g_it++;
                    }
                } else {
                    if (isActive(t, *g_it)) {
                        return *g_it;
                    } else {
                        o_it++;
                        g_it++;
                    }
                }
            }
        }

        return NO_OBJECT;
    }

    void add(transaction_id t, object_id o) {
        if (t < mGuaranteedLowestId) {
            throw std::runtime_error("Can't add or remove data before the lowest id.");
        }

        if (isActive(t,o)) {
            return;
        }

        mTransToObj[t][o] = true;
        mObjToTrans[o][t] = true;
    }

    void remove(transaction_id t, object_id o) {
        if (t < mGuaranteedLowestId) {
            throw std::runtime_error("Can't add or remove data before the lowest id.");
        }

        if (!isActive(t,o)) {
            return;
        }

        mTransToObj[t][o] = false;
        mObjToTrans[o][t] = false;
    }

    size_t transactionCount() const {
        return mTransToObj.size();
    }

    void dumpState() const {
        std::cout << "lowest = " << mGuaranteedLowestId << "\n";

        std::cout << "mPresentAtLowestId:\n";
        for (auto i: mPresentAtLowestId) {
            std::cout << "    " << i << "\n";
        }

        for (auto t: mTransToObj) {
            std::cout << "transaction_id: " << t.first << std::endl;
            for (auto oAndA: t.second) {
                std::cout << "   o=" << oAndA.first << " and " << (oAndA.second ? "add":"rem") << "\n";
            }
        }

        for (auto t: mObjToTrans) {
            std::cout << "object_id: " << t.first << std::endl;
            for (auto oAndA: t.second) {
                std::cout << "   t=" << oAndA.first << " and " << (oAndA.second ? "add":"rem") << "\n";
            }
        }
    }

    size_t totalEntryCount() const {
        size_t res = mPresentAtLowestId.size();

        for (auto& tidAndObjects: mTransToObj) {
            res += tidAndObjects.second.size();
        }

        return res;
    }

private:
    transaction_id mGuaranteedLowestId; //the lowest transaction anyone will ever ask us about

    //a collection of objects present at the 'lowest id'
    std::set<object_id> mPresentAtLowestId;

    //for each transaction, what did we add? only populated for transactions above the lowest
    //guaranteed id
    std::map<transaction_id, std::map<object_id, bool> > mTransToObj;

    //for each object, the transactions where it was added (true) and removed (false)
    std::map<object_id, std::map<transaction_id, bool> > mObjToTrans;
};
