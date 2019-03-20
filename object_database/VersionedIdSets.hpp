#pragma once

#include <memory>
#include <set>

#include "VersionedIdSet.hpp"

/*************

VersionedIdSets - a map from an index_key to VersionedIdSet

*************/

class VersionedIdSets {
public:
    typedef std::pair<int64_t, std::string> index_key_type;

    VersionedIdSets()
    {
    }

    void addTransaction(
            transaction_id transaction,
            const std::map<index_key_type, std::vector<object_id> >& adds,
            const std::map<index_key_type, std::vector<object_id> >& removes
            )
    {
        for (const auto& hashAndObjects: adds) {
            auto& idSet(idSetFor(hashAndObjects.first));

            for (auto objId: hashAndObjects.second) {
                idSet.add(transaction, objId);
            }

            mTransToIndices[transaction].insert(hashAndObjects.first);
        }

        for (const auto& hashAndObjects: removes) {
            auto& idSet(idSetFor(hashAndObjects.first));

            for (auto objId: hashAndObjects.second) {
                idSet.remove(transaction, objId);
            }

            mTransToIndices[transaction].insert(hashAndObjects.first);
        }
    }

    /********
    get or create a VersionedIdSet for a particular hash value
    ********/
    VersionedIdSet& idSetFor(index_key_type h) {
        auto it = mIdSets.find(h);
        if (it == mIdSets.end()) {
            mIdSets[h].reset(new VersionedIdSet());

            return *mIdSets[h];
        }

        return *it->second;
    }

    /*******
    lookup exactly one value, or return NO_OBJECT if we can't find it
    *******/
    object_id lookupOne(index_key_type value, transaction_id transaction) {
        auto it = mIdSets.find(value);
        if (it != mIdSets.end()) {
            return it->second->lookupOne(transaction);
        }

        return NO_OBJECT;
    }

    /*******
    lookup the lowest value active at transaction 'transaction' or return NO_OBJECT
    *******/
    object_id lookupFirst(index_key_type value, transaction_id transaction) {
        auto it = mIdSets.find(value);
        if (it != mIdSets.end()) {
            return it->second->lookupFirst(transaction);
        }

        return NO_OBJECT;
    }

    /*******
    lookup the next value active at transaction 'transaction' or return NO_OBJECT
    *******/
    object_id lookupNext(index_key_type value, transaction_id transaction, object_id o) {
        auto it = mIdSets.find(value);
        if (it != mIdSets.end()) {
            return it->second->lookupNext(transaction, o);
        }

        return NO_OBJECT;
    }

    transaction_id firstTransaction() const {
        if (!mTransToIndices.size()) {
            return NO_TRANSACTION;
        }

        return mTransToIndices.begin()->first;
    }

    std::shared_ptr<VersionedIdSet> idSetPtrFor(index_key_type index) {
        if (!mIdSets[index]) {
            mIdSets[index].reset(new VersionedIdSet());
        }

        return mIdSets[index];
    }

private:
    std::map<transaction_id, std::set<index_key_type> > mTransToIndices;

    std::set<index_key_type> mNeedsCleanup;

    std::map<index_key_type, std::shared_ptr<VersionedIdSet> > mIdSets;

};