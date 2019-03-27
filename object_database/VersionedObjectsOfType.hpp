#pragma once

#include "DictInstance.hpp"
#include "Common.hpp"
/*************

VersionedObjectsOfType stores a collection of TypedPython objects all of a single type,
indexed by version and object id.

We represent the data using four mappings:
* a map from objectid to <version, object_data> storing the largest version of each object
* a map from objectid to version containing the bottommost version if there are inner versions
* a dictionary representing the next and prior version number for each (object,version) pair
    provided we have more than one. Otherwise no entries exist.
* a dictionary storing the actual object for each interior (object,version) pair.

We implement these mappings using typed_python Dicts so that compiled
nativepython can also read from them.

*************/

class VersionedObjectsOfType {
    enum { NO_TRANSACTION = -1 };

    class VersionAndObjectData {
    public:
        transaction_id version;
        transaction_id deletedAsOf;
        unsigned char object_data[];
    };

    class ObjectAndVersion {
    public:
        ObjectAndVersion(object_id objId, transaction_id verId) :
                objectId(objId),
                version(verId)
        {
        }

        object_id objectId;
        transaction_id version;
    };

    class NextAndPrior {
    public:
        object_id nextId;
        object_id priorId;
    };

    class ObjectData {
    public:
        unsigned char object_data[8]; //can be any number, but need something since otherwise this is empty
    };

public:
    VersionedObjectsOfType(Type* type) :
            m_type(type),
            m_guaranteed_lowest_id(NO_TRANSACTION),
            m_top_objects(Int64::Make(), Tuple::Make(std::vector<Type*>({Int64::Make(), Int64::Make(), type}))),
            m_lowest_versions(Int64::Make(), Int64::Make()),
            m_next_and_prior(
                Tuple::Make(std::vector<Type*>({Int64::Make(),Int64::Make()})),
                Tuple::Make(std::vector<Type*>({Int64::Make(),Int64::Make()}))
                ),
            m_data(Tuple::Make(std::vector<Type*>({Int64::Make(),Int64::Make()})), type)
    {
    }

    bool empty() const {
        return m_top_objects.size() == 0;
    }

    transaction_id getGuaranteedLowestId() const {
        return m_guaranteed_lowest_id;
    }

    void moveGuaranteedLowestIdForward(transaction_id t) {
        if (t <= m_guaranteed_lowest_id) {
            return;
        }

        m_guaranteed_lowest_id = t;

        while (m_version_numbers_to_check.size() && m_version_numbers_to_check.begin()->first < t) {
            // this is the ID we're consuming, which is the lowest id mentioned in the
            // entire object.
            transaction_id lowestId = m_version_numbers_to_check.begin()->first;

            std::set<object_id> toCheck;
            std::swap(toCheck, m_version_numbers_to_check.begin()->second);
            m_version_numbers_to_check.erase(lowestId);

            for (auto objectId: toCheck) {
                removeLowestIfPossible(objectId);
            }
        }
    }

    Type* getType() const {
        return m_type;
    }

    std::pair<instance_ptr, transaction_id> best(object_id objectId, transaction_id version) {
        if (version < m_guaranteed_lowest_id) {
            return std::pair<instance_ptr, transaction_id>(nullptr, NO_TRANSACTION);
        }

        VersionAndObjectData* topObjectPtr = m_top_objects.lookupKey(objectId);

        if (!topObjectPtr) {
            return std::pair<instance_ptr, transaction_id>(nullptr, NO_TRANSACTION);
        }

        // check if the object is deleted
        if (topObjectPtr->deletedAsOf != NO_TRANSACTION && topObjectPtr->deletedAsOf <= version) {
            return std::pair<instance_ptr, transaction_id>(nullptr, topObjectPtr->deletedAsOf);
        }

        if (topObjectPtr->version <= version) {
            return std::pair<instance_ptr, transaction_id>(topObjectPtr->object_data, topObjectPtr->version);
        }

        transaction_id curVersion = topObjectPtr->version;

        while (curVersion > version) {
            NextAndPrior* curNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, curVersion));

            if (!curNAP) {
                return std::pair<instance_ptr, transaction_id>(nullptr, NO_TRANSACTION);
            }

            curVersion = curNAP->priorId;
        }

        if (curVersion == NO_TRANSACTION) {
            return std::pair<instance_ptr, transaction_id>(nullptr, NO_TRANSACTION);
        }

        return std::pair<instance_ptr, transaction_id>(
            m_data.lookupKey(ObjectAndVersion(objectId,curVersion))->object_data,
            curVersion
            );
    }

    //consume any values in the object that are _lower_ than 'version'
    void removeLowestIfPossible(object_id objectId) {
        VersionAndObjectData* topObjectPtr = m_top_objects.lookupKey(objectId);

        if (!topObjectPtr) {
            return;
        }

        if (topObjectPtr->deletedAsOf != NO_TRANSACTION && topObjectPtr->deletedAsOf <= m_guaranteed_lowest_id) {
            removeObject(objectId);
            return;
        }

        transaction_id* lowestIdPtr = m_lowest_versions.lookupKey(objectId);

        if (!lowestIdPtr || *lowestIdPtr >= m_guaranteed_lowest_id) {
            return;
        }

        transaction_id lowestId = *lowestIdPtr;

        //check whether the _next_ value is also behind m_guaranteed_lowest_id. If so, we can delete this one
        NextAndPrior* firstNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, lowestId));

        // if the next object is above the guarantee, we need this version so we can fill in any requests
        // that occur betwen m_guaranteed_lowest_id and next transaction
        transaction_id nextId = firstNAP->nextId;

        if (nextId > m_guaranteed_lowest_id) {
            return;
        }

        *lowestIdPtr = nextId;

        m_data.deleteKey(ObjectAndVersion(objectId, lowestId));

        if (nextId == topObjectPtr->version) {
            //we now only have a top object. so delete the pointer-links
            m_next_and_prior.deleteKey(ObjectAndVersion(objectId, topObjectPtr->version));
            m_next_and_prior.deleteKey(ObjectAndVersion(objectId, nextId));
            m_lowest_versions.deleteKey(objectId);
        } else {
            NextAndPrior* nextNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, nextId));
            nextNAP->priorId = NO_TRANSACTION;
        }
    }


    //remove all traces of an object from the transaction stream
    void removeObject(object_id objectId) {
        VersionAndObjectData* topObjectPtr = m_top_objects.lookupKey(objectId);

        if (!topObjectPtr) {
            return;
        }

        dropObjAndVersion(objectId, topObjectPtr->version);
        m_top_objects.deleteKey(objectId);

        transaction_id* lowestTidPtr = m_lowest_versions.lookupKey(objectId);

        if (!lowestTidPtr) {
            return;
        }

        transaction_id lowestTid = *lowestTidPtr;
        m_lowest_versions.deleteKey(objectId);

        while (lowestTid != NO_TRANSACTION) {
            dropObjAndVersion(objectId, lowestTid);
            m_data.deleteKey(ObjectAndVersion(objectId, lowestTid));
            NextAndPrior nap = *m_next_and_prior.lookupKey(ObjectAndVersion(objectId, lowestTid));
            m_next_and_prior.deleteKey(ObjectAndVersion(objectId, lowestTid));

            lowestTid = nap.nextId;
        }
    }

    // mark an object 'deleted' as of a particular version number. once deleted,
    // it can't come back. transactions coming it at or above it will fail.
    bool markDeleted(object_id objectId, transaction_id version) {
        if (version < m_guaranteed_lowest_id) {
            return false;
        }

        VersionAndObjectData* topObjectPtr = m_top_objects.lookupKey(objectId);

        // makes no sense to delete an object that doesn't exist
        if (!topObjectPtr) {
            return false;
        }

        if (topObjectPtr->deletedAsOf != NO_TRANSACTION) {
            // re-deleting is a no-op, anything else an error
            return topObjectPtr->deletedAsOf == version;
        }

        // makes no sense to delete before the current version
        if (topObjectPtr->version >= version) {
            return false;
        }

        topObjectPtr->deletedAsOf = version;

        if (version < m_guaranteed_lowest_id) {
            removeObject(objectId);
        } else {
            m_version_numbers_to_check[version].insert(objectId);
        }

        return true;
    }

    /****
    adds an object by id. If the object already exists or an error occurs,
    returns 'false' and does nothing. Otherwise, add it and return 'true'.

    instance must point to the data for a valid instance of type 'm_type', or
    else undefined (and bad) things will happen.

    The version number and object ids must be nonnegative.
    *****/
    bool add(object_id objectId, object_id version, instance_ptr instance) {
        if (version < m_guaranteed_lowest_id) {
            return false;
        }

        //the proper lookup key in the object/version table.
        ObjectAndVersion objIdAndVersion(objectId, version);

        VersionAndObjectData* topObjectPtr = m_top_objects.lookupKey(objectId);

        if (!topObjectPtr) {
            //this is a completely new object
            topObjectPtr = m_top_objects.insertKey(objectId);
            topObjectPtr->version = version;
            topObjectPtr->deletedAsOf = NO_TRANSACTION;
            m_type->copy_constructor(topObjectPtr->object_data, instance);
            m_version_numbers_to_check[version].insert(objectId);
            return true;
        }

        if (topObjectPtr->deletedAsOf != NO_TRANSACTION &&
                            topObjectPtr->deletedAsOf <= version)  {
            // an error to add data above the deleted version.
            return false;
        }

        ObjectAndVersion topObject(objectId, topObjectPtr->version);

        //check if the object already exists then.
        if (topObject.version == version) {
            return false;
        }

        //are we ahead of this object?
        if (topObject.version < version) {
            //move the top object into the backstack
            ObjectData* interior = m_data.insertKey(topObject);

            //the value at this transaction_it is now "non-top"
            registerObjectAndVersion(objectId, topObjectPtr->version);

            m_type->swap(interior->object_data, topObjectPtr->object_data);
            m_type->copy_constructor(topObjectPtr->object_data, instance);
            topObjectPtr->version = version;

            //insert a link for the new top object
            NextAndPrior* insertedNAP = m_next_and_prior.insertKey(objIdAndVersion);
            insertedNAP->nextId = NO_TRANSACTION;
            insertedNAP->priorId = topObject.version;

            //see if we already had a prior
            NextAndPrior* oldTopNAP = m_next_and_prior.lookupKey(topObject);

            if (oldTopNAP) {
                //we did. Just point it at us
                oldTopNAP->nextId = version;
            } else {
                //this is the first time we have multiple values at this object
                //so we need to insert a pointer for it.
                *m_lowest_versions.lookupOrInsert(objectId) = topObject.version;

                oldTopNAP = m_next_and_prior.insertKey(topObject);
                oldTopNAP->priorId = NO_TRANSACTION;
                oldTopNAP->nextId = version;
            }

            m_version_numbers_to_check[version].insert(objectId);

            return true;
        }

        //check if it exists already
        if (m_data.lookupKey(objIdAndVersion)) {
            return false;
        }

        //insert the value
        ObjectData* interior = m_data.insertKey(objIdAndVersion);
        m_type->copy_constructor(interior->object_data, instance);

        //this value is nontop
        registerObjectAndVersion(objectId, version);

        //update the interior version chains
        transaction_id* lowestVersion = m_lowest_versions.lookupKey(objectId);

        //this is an interior insertion
        if (!lowestVersion) {
            //add both links
            NextAndPrior* curTopNAP = m_next_and_prior.insertKey(topObject);
            curTopNAP->nextId = NO_TRANSACTION;
            curTopNAP->priorId = version;

            NextAndPrior* curInteriorNAP = m_next_and_prior.insertKey(objIdAndVersion);
            curInteriorNAP->nextId = topObject.version;
            curInteriorNAP->priorId = NO_TRANSACTION;

            //update the lowest version
            *m_lowest_versions.insertKey(objectId) = objIdAndVersion.version;

            m_version_numbers_to_check[version].insert(objectId);

            return true;
        }

        if (*lowestVersion > version) {
            //this is the new lowest. Update 'lowest'
            NextAndPrior* curNextNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, *lowestVersion));
            curNextNAP->priorId = version;

            //insert ourselves
            NextAndPrior* curInteriorNAP = m_next_and_prior.insertKey(objIdAndVersion);
            curInteriorNAP->nextId = *lowestVersion;
            curInteriorNAP->priorId = NO_TRANSACTION;

            //update the bottom pointer
            *lowestVersion = version;

            m_version_numbers_to_check[version].insert(objectId);

            return true;
        }

        //this is in the interior
        transaction_id curVersion = topObject.version;
        NextAndPrior* curNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, curVersion));

        while (curNAP->priorId > version) {
            curVersion = curNAP->priorId;
            curNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, curVersion));
        }

        transaction_id priorVersion = curNAP->priorId;

        NextAndPrior* curInteriorNAP = m_next_and_prior.insertKey(objIdAndVersion);

        //this can reallocate the table, so we need to reload both
        curNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, curVersion));
        NextAndPrior* priorNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, curNAP->priorId));


        curInteriorNAP->nextId = curVersion;
        curInteriorNAP->priorId = curNAP->priorId;

        curNAP->priorId = version;
        priorNAP->nextId = version;

        m_version_numbers_to_check[version].insert(objectId);

        return true;
    }

    void dropObjAndVersion(object_id oid, transaction_id tid) {
        m_version_numbers_to_check[tid].erase(oid);
        if (m_version_numbers_to_check[tid].size() == 0) {
            m_version_numbers_to_check.erase(tid);
        }
    }

    void registerObjectAndVersion(object_id oid, transaction_id tid) {
        m_version_numbers_to_check[tid].insert(oid);
    }

private:
    Type* m_type;   //the actual type of the objects we contain

    transaction_id m_guaranteed_lowest_id; //the lowest transaction anyone will ever ask us about

    //primary datastructure - populated for all objects we know. The transaction_ids in here
    //may be lower than the m_guaranteed_lowest_id - we only roll them forward when we need to.
    DictInstance<object_id, VersionAndObjectData> m_top_objects;

    //for all objects with multiple versions, the lowest
    DictInstance<object_id, transaction_id> m_lowest_versions;

    //for all objects with multiple versions, at each version,
    //the next and prior version number
    DictInstance<ObjectAndVersion, NextAndPrior> m_next_and_prior;

    //for all non-top object versions, the object data
    DictInstance<ObjectAndVersion, ObjectData> m_data;

    //for each transaction, a list of objects we want to check when that version number
    //gets consumed by the m_guaranteed_lowest_id, in case we want to delete things.
    std::map<transaction_id, std::set<object_id> > m_version_numbers_to_check;
};