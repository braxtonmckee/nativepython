#pragma once

#include "../typed_python/AllTypes.hpp"
#include "../typed_python/Instance.hpp"
#include "../typed_python/DictType.hpp"

template<class key_type, class value_type>
class DictInstance {
public:
    DictInstance(Type* keyType, Type* valueType) {
        mInstance = Instance::create(Dict::Make(keyType, valueType));
    }

    value_type* lookupKey(const key_type& key) {
        return (value_type*)((Dict*)mInstance.type())->lookupValueByKey(mInstance.data(), (instance_ptr)&key);
    }

    value_type* insertKey(const key_type& key) {
        return (value_type*)((Dict*)mInstance.type())->insertKey(mInstance.data(), (instance_ptr)&key);
    }

    bool deleteKey(const key_type& key) {
        return ((Dict*)mInstance.type())->deleteKey(mInstance.data(), (instance_ptr)&key);
    }

    value_type* lookupOrInsert(const key_type& key) {
        auto resPtr = lookupKey(key);

        if (resPtr) {
            return resPtr;
        }

        return insertKey(key);
    }

private:
    Instance mInstance;
};

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
    enum { NO_VERSION = -1 };

    class VersionAndObjectData {
    public:
        int64_t version;
        unsigned char object_data[];
    };

    class ObjectAndVersion {
    public:
        ObjectAndVersion(int64_t objId, int64_t verId) :
                objectId(objId),
                version(verId)
        {
        }

        int64_t objectId;
        int64_t version;
    };

    class NextAndPrior {
    public:
        int64_t nextId;
        int64_t priorId;
    };

    class ObjectData {
    public:
        unsigned char object_data[8]; //can be any number, but need something since otherwise this is empty
    };

public:
    VersionedObjectsOfType(Type* type) :
            m_type(type),
            m_top_objects(Int64::Make(), Tuple::Make(std::vector<Type*>({Int64::Make(), type}))),
            m_lowest_versions(Int64::Make(), Int64::Make()),
            m_next_and_prior(
                Tuple::Make(std::vector<Type*>({Int64::Make(),Int64::Make()})),
                Tuple::Make(std::vector<Type*>({Int64::Make(),Int64::Make()}))
                ),
            m_data(Tuple::Make(std::vector<Type*>({Int64::Make(),Int64::Make()})), type)
    {
    }

    Type* getType() const {
        return m_type;
    }

    std::pair<instance_ptr, int64_t> best(int64_t objectId, int64_t version) {
        VersionAndObjectData* topObjectPtr = m_top_objects.lookupKey(objectId);

        if (!topObjectPtr) {
            return std::pair<instance_ptr, int64_t>(nullptr, 0);
        }

        if (topObjectPtr->version <= version) {
            return std::pair<instance_ptr, int64_t>(topObjectPtr->object_data, topObjectPtr->version);
        }

        int64_t curVersion = topObjectPtr->version;

        while (curVersion > version) {
            NextAndPrior* curNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, curVersion));

            if (!curNAP) {
                return std::pair<instance_ptr, int64_t>(nullptr, 0);
            }

            curVersion = curNAP->priorId;
        }

        if (curVersion == NO_VERSION) {
            return std::pair<instance_ptr, int64_t>(nullptr, 0);
        }

        return std::pair<instance_ptr, int64_t>(
            m_data.lookupKey(ObjectAndVersion(objectId,curVersion))->object_data,
            curVersion
            );
    }

    //remove a specific version from the set
    bool remove(int64_t objectId, int64_t version) {
        VersionAndObjectData* topObjectPtr = m_top_objects.lookupKey(objectId);

        if (!topObjectPtr) {
            return false;
        }

        if (topObjectPtr->version == version) {
            //see if a new top object exists
            NextAndPrior* topNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, version));
            if (topNAP) {
                int64_t newTopVersion = topNAP->priorId;
                m_next_and_prior.deleteKey(ObjectAndVersion(objectId, version));

                //migrate this to the top object
                ObjectData* data = m_data.lookupKey(ObjectAndVersion(objectId, newTopVersion));
                m_type->swap(data->object_data, topObjectPtr->object_data);
                topObjectPtr->version = newTopVersion;
                m_data.deleteKey(ObjectAndVersion(objectId, newTopVersion));

                //now update the pointer graph
                NextAndPrior* newTopNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, newTopVersion));

                if (newTopNAP->priorId == NO_VERSION) {
                    //this was the last version
                    m_next_and_prior.deleteKey(ObjectAndVersion(objectId, newTopVersion));
                    m_lowest_versions.deleteKey(objectId);
                } else {
                    newTopNAP->nextId = NO_VERSION;
                }
            } else {
                m_top_objects.deleteKey(objectId);
            }

            return true;
        }

        //we're not the top object. Are we in the data set?
        ObjectData* data = m_data.lookupKey(ObjectAndVersion(objectId, version));
        if (!data) {
            return false;
        }

        m_data.deleteKey(ObjectAndVersion(objectId, version));

        //update the pointer graph
        NextAndPrior ourNAP = *m_next_and_prior.lookupKey(ObjectAndVersion(objectId, version));

        //delete the entry. note we copy the object, not the pointer
        m_next_and_prior.deleteKey(ObjectAndVersion(objectId, version));

        if (ourNAP.nextId == topObjectPtr->version && ourNAP.priorId == NO_VERSION) {
            //we now only have a top object. so delete the pointer-links
            m_next_and_prior.deleteKey(ObjectAndVersion(objectId, topObjectPtr->version));
            m_lowest_versions.deleteKey(objectId);
        } else {
            m_next_and_prior.lookupKey(ObjectAndVersion(objectId, ourNAP.nextId))->priorId = ourNAP.priorId;

            if (ourNAP.priorId == NO_VERSION) {
                //we have no prior. Update the lowest-version
                *m_lowest_versions.lookupKey(objectId) = ourNAP.nextId;
            } else {
                m_next_and_prior.lookupKey(ObjectAndVersion(objectId, ourNAP.priorId))->nextId = ourNAP.nextId;
            }
        }

        return true;
    }

    /****
    adds an object by id. If the object already exists, returns 'false'
    and does nothing. Otherwise, add it and return 'true'.

    instance must point to the data for a valid instance of type 'm_type',
    or else undefined (and bad) things will happen.

    The version number and object ids must be nonnegative.
    *****/
    bool add(int64_t objectId, int64_t version, instance_ptr instance) {
        //the proper lookup key in the object/version table.
        ObjectAndVersion objIdAndVersion(objectId, version);

        VersionAndObjectData* topObjectPtr = m_top_objects.lookupKey(objectId);

        if (!topObjectPtr) {
            //this is completely new
            topObjectPtr = m_top_objects.insertKey(objectId);
            topObjectPtr->version = version;
            m_type->copy_constructor(topObjectPtr->object_data, instance);
            return true;
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

            m_type->swap(interior->object_data, topObjectPtr->object_data);
            m_type->copy_constructor(topObjectPtr->object_data, instance);
            topObjectPtr->version = version;

            //insert a link for the new top object
            NextAndPrior* insertedNAP = m_next_and_prior.insertKey(objIdAndVersion);
            insertedNAP->nextId = NO_VERSION;
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
                oldTopNAP->priorId = NO_VERSION;
                oldTopNAP->nextId = version;
            }

            return true;
        }

        //check if it exists already
        if (m_data.lookupKey(objIdAndVersion)) {
            return false;
        }

        //insert the value
        ObjectData* interior = m_data.insertKey(objIdAndVersion);
        m_type->copy_constructor(interior->object_data, instance);

        //update the interior version chains
        int64_t* lowestVersion = m_lowest_versions.lookupKey(objectId);

        //this is an interior insertion
        if (!lowestVersion) {
            //add both links
            NextAndPrior* curTopNAP = m_next_and_prior.insertKey(topObject);
            curTopNAP->nextId = NO_VERSION;
            curTopNAP->priorId = version;

            NextAndPrior* curInteriorNAP = m_next_and_prior.insertKey(objIdAndVersion);
            curInteriorNAP->nextId = topObject.version;
            curInteriorNAP->priorId = NO_VERSION;

            //update the lowest version
            *m_lowest_versions.insertKey(objectId) = objIdAndVersion.version;

            return true;
        }

        if (*lowestVersion > version) {
            //this is the new lowest. Update 'lowest'
            NextAndPrior* curNextNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, *lowestVersion));
            curNextNAP->priorId = version;

            //insert ourselves
            NextAndPrior* curInteriorNAP = m_next_and_prior.insertKey(objIdAndVersion);
            curInteriorNAP->nextId = *lowestVersion;
            curInteriorNAP->priorId = NO_VERSION;

            //update the bottom pointer
            *lowestVersion = version;

            return true;
        }

        //this is in the interior
        int64_t curVersion = topObject.version;
        NextAndPrior* curNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, curVersion));

        while (curNAP->priorId > version) {
            curVersion = curNAP->priorId;
            curNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, curVersion));
        }

        int64_t priorVersion = curNAP->priorId;

        NextAndPrior* curInteriorNAP = m_next_and_prior.insertKey(objIdAndVersion);

        //this can reallocate the table, so we need to reload both
        curNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, curVersion));
        NextAndPrior* priorNAP = m_next_and_prior.lookupKey(ObjectAndVersion(objectId, curNAP->priorId));


        curInteriorNAP->nextId = curVersion;
        curInteriorNAP->priorId = curNAP->priorId;

        curNAP->priorId = version;
        priorNAP->nextId = version;

        return true;
    }


private:
    Type* m_type;   //the actual type of the objects we contain

    DictInstance<int64_t, VersionAndObjectData> m_top_objects;

    DictInstance<int64_t, int64_t> m_lowest_versions;

    DictInstance<ObjectAndVersion, NextAndPrior> m_next_and_prior;

    DictInstance<ObjectAndVersion, ObjectData> m_data;
};