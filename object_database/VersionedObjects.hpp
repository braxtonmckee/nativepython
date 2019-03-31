#pragma once

#include <memory>
#include <map>

#include "../typed_python/Type.hpp"
#include "Common.hpp"
#include "VersionedObjectsOfType.hpp"

/*************

VersionedObjects stores a collection of TypedPython objects that are each
indexed by a pair of int64s (objectid, and fieldid) and a version.

We provide functionality to
* perform a fast lookup of values by (object,field,version) tuples
* discarding values below a given global version
* merge in data with new version numbers
* tag that data exists with a given version number but that's not loaded

*************/

class VersionedObjects {
public:
    typedef std::pair<field_id, index_value> index_id;

    void defineValueType(field_id fieldId, Type* type) {
        if (m_field_to_versioned_objects.find(fieldId) != m_field_to_versioned_objects.end()) {
            return;
        }

        m_field_to_versioned_objects[fieldId].reset(new VersionedObjectsOfType(type));
    }

    Type* valueTypeForField(field_id fieldId) {
        auto it = m_field_to_versioned_objects.find(fieldId);

        if (it == m_field_to_versioned_objects.end()) {
            return nullptr;
        }

        return it->second->getType();
    }

    std::pair<instance_ptr, transaction_id> bestObjectVersion(field_id fieldId, object_id objectId, transaction_id version) {
        return m_field_to_versioned_objects[fieldId]->best(objectId, version);
    }

    bool addObjectVersion(field_id fieldId, object_id oid, transaction_id tid, instance_ptr instance) {
        //mark this field on this transaction so we can garbage collect it
        m_fields_needing_check.insert(std::make_pair(tid,fieldId));

        return m_field_to_versioned_objects[fieldId]->add(oid, tid, instance);
    }

    bool markObjectVersionDeleted(field_id fieldId, object_id objectId, transaction_id version) {
        //mark this field on this transaction so we can garbage collect it
        m_fields_needing_check.insert(std::make_pair(version, fieldId));

        return m_field_to_versioned_objects[fieldId]->markDeleted(objectId, version);
    }

    object_id indexLookupOne(field_id fid, index_value i, transaction_id t) {
        return m_index_to_versioned_id_sets[index_id(fid,i)].lookupOne(t);
    }

    object_id indexLookupFirst(field_id fid, index_value i, transaction_id t) {
        return indexLookupNext(fid, i, t, NO_OBJECT);
    }

    object_id indexLookupNext(field_id fid, index_value i, transaction_id t, object_id o) {
        return m_index_to_versioned_id_sets[index_id(fid, i)].lookupNext(t, o);
    }

    void indexAdd(field_id fid, index_value i, transaction_id t, object_id o) {
        return m_index_to_versioned_id_sets[index_id(fid, i)].add(t,o);
    }

    void indexRemove(field_id fid, index_value i, transaction_id t, object_id o) {
        return m_index_to_versioned_id_sets[index_id(fid, i)].remove(t,o);
    }

    void moveGuaranteedLowestIdForward(transaction_id t) {
        while (m_fields_needing_check.size() && m_fields_needing_check.begin()->first < t) {
            //grab the field id and consume it from the queue
            field_id fieldId = m_fields_needing_check.begin()->second;
            m_fields_needing_check.erase(m_fields_needing_check.begin());

            m_field_to_versioned_objects[fieldId]->moveGuaranteedLowestIdForward(t);
        }

        while (m_indices_needing_check.size() && m_indices_needing_check.begin()->first < t) {
            //grab the field id and consume it from the queue
            index_id indexId = m_indices_needing_check.begin()->second;
            m_indices_needing_check.erase(m_indices_needing_check.begin());

            auto& versionedIdSet = m_index_to_versioned_id_sets[indexId];

            transaction_id next = versionedIdSet.moveGuaranteedLowestIdForward(t);

            if (next != NO_TRANSACTION) {
                m_indices_needing_check.insert(std::make_pair(next, indexId));
            }
        }
    }

private:
    std::map<field_id, std::shared_ptr<VersionedObjectsOfType> > m_field_to_versioned_objects;

    std::map<index_id, VersionedIdSet> m_index_to_versioned_id_sets;

    std::set<std::pair<transaction_id, field_id> > m_fields_needing_check;

    std::set<std::pair<transaction_id, index_id> > m_indices_needing_check;
};