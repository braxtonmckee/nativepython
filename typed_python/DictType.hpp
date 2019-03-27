#pragma once

#include "Type.hpp"
#include "ReprAccumulator.hpp"

#include <unordered_map>

class Dict : public Type {
public:
    class layout {
    public:
        layout() :
            refcount(0),
            items(nullptr),
            items_populated(nullptr),
            items_reserved(0),
            top_item_slot(0),

            hash_table_slots(nullptr),
            hash_table_hashes(nullptr),
            hash_table_size(0),
            hash_table_count(0),
            hash_table_empty_slots(0)
        {
        }

        enum { EMPTY = -1, DELETED = -2 };

        //return the index of the object indexed by 'hash', or -1
        template<class eq_func>
        int32_t find(int32_t kv_pair_size, int32_t hash, const eq_func& compare) {
            if (!hash_table_slots) {
                return -1;
            }

            int32_t offset = hash % hash_table_size;

            while (true) {
                //slot is empty
                int32_t slot = hash_table_slots[offset];

                if (slot == EMPTY) {
                    return -1;
                }

                if (slot != DELETED && hash_table_hashes[offset] == hash && compare(items + kv_pair_size * slot)) {
                    return slot;
                }

                offset = nextOffset(offset);
            }
        }

        //linear search
        int32_t nextOffset(int32_t offset) const {
            offset += 1;
            if (offset >= hash_table_size) {
                offset = 0;
            }
            return offset;
        }

        //add an item to the hash table
        void add(int32_t hash, int32_t slot) {
            if (hash_table_count * 2 + 1 > hash_table_size || hash_table_empty_slots < hash_table_size / 4 + 1) {
                resizeTable();
            }

            int32_t offset = hash % hash_table_size;
            while (true) {
                if (hash_table_slots[offset] == EMPTY || hash_table_slots[offset] == DELETED) {
                    if (hash_table_slots[offset] == EMPTY) {
                        hash_table_empty_slots--;
                    }

                    hash_table_slots[offset] = slot;
                    hash_table_hashes[offset] = hash;
                    items_populated[slot] = 1;
                    hash_table_count++;
                    return;
                }

                offset = nextOffset(offset);
            }
        }

        //remove an item with the given hash. returning the item slot where it lived.
        //-1 if not found
        template<class eq_func>
        int32_t remove(int32_t kv_pair_size, int32_t hash, const eq_func& compare) {
            if (!hash_table_slots) {
                return -1;
            }

            if (items_reserved > (hash_table_count + 2) * 4) {
                compressItemTable(kv_pair_size);
            }

            //compress the hashtable if it's really empty
            if (hash_table_count < hash_table_size / 8) {
                resizeTable();
            }

            int32_t offset = hash % hash_table_size;

            while (true) {
                int32_t slot = hash_table_slots[offset];

                if (slot == EMPTY) {
                    //we never found the item
                    return -1;
                }

                if (slot != DELETED && compare(items + kv_pair_size * slot)) {
                    items_populated[slot] = 0;

                    hash_table_slots[offset] = DELETED;
                    hash_table_hashes[offset] = -1;
                    hash_table_count -= 1;

                    return slot;
                }

                offset = nextOffset(offset);
            }
        }

        void compressItemTable(size_t kv_pair_size) {
            std::vector<int32_t> newItemPositions;
            int32_t count_so_far = 0;

            for (long k = 0; k < items_reserved; k++) {
                if (items_populated[k]) {
                    newItemPositions.push_back(count_so_far);

                    if (k != count_so_far) {
                        items_populated[count_so_far] = 1;
                        items_populated[k] = 0;

                        memcpy(items + kv_pair_size * count_so_far, items + kv_pair_size * k, kv_pair_size);
                    }

                    count_so_far++;
                } else {
                    newItemPositions.push_back(-1);
                }
            }

            items_reserved = count_so_far;
            items_populated = (uint8_t*)realloc(items_populated, count_so_far);
            items = (uint8_t*)realloc(items, count_so_far * kv_pair_size);
            top_item_slot = items_reserved;

            for (long k = 0; k < hash_table_size; k++) {
                if (hash_table_slots[k] >= 0) {
                    if (hash_table_slots[k] >= newItemPositions.size()) {
                        throw std::runtime_error("corrupt slot");
                    }

                    hash_table_slots[k] = newItemPositions[hash_table_slots[k]];

                    if (hash_table_slots[k] < 0) {
                        throw std::runtime_error("invalid slot");
                    }
                }
            }

            for (long k = 0; k < hash_table_size; k++) {
                if (hash_table_slots[k] >= 0) {
                    if (hash_table_slots[k] >= items_reserved) {
                        throw std::runtime_error("failed during compression");
                    }
                }
            }
        }

        int32_t allocateNewSlot(size_t kv_pair_size) {
            if (!items) {
                items = (uint8_t*)malloc(4 * kv_pair_size);
                items_populated = (uint8_t*)malloc(4);
                items_reserved = 4;
                top_item_slot = 0;
                for (long k = 0; k < items_reserved; k++) {
                    items_populated[k] = 0;
                }
            }

            while (top_item_slot >= items_reserved) {
                size_t old_reserved = items_reserved;
                items_reserved = items_reserved * 1.25 + 1;
                items = (uint8_t*)realloc(items, kv_pair_size * items_reserved);
                items_populated = (uint8_t*)realloc(items_populated, items_reserved);

                for (long k = old_reserved; k < items_reserved; k++) {
                    items_populated[k] = 0;
                }
            }

            return top_item_slot++;
        }

        int32_t computeNextPrime(int32_t p) {
            static std::vector<int32_t> primes;
            if (!primes.size()) {
                primes.push_back(2);
            }

            auto isprime = [&](int32_t candidate) {
                for (auto d: primes) {
                    if (candidate % d == 0) {
                        return false;
                    }
                    if (d*d > candidate) {
                        return true;
                    }
                }
                throw std::logic_error("Expected to clear the primes list.");
            };

            while (true) {
                while (primes.back() * primes.back() < p) {
                    int32_t cur = primes.back() + 1;
                    while (!isprime(cur)) {
                        cur++;
                    }
                    primes.push_back(cur);
                }

                if (isprime(p)) {
                    return p;
                }

                p++;
            }
        }

        void resizeTable() {
            if (!hash_table_slots) {
                hash_table_slots = (int32_t*)malloc(7 * sizeof(int32_t));
                hash_table_hashes = (int32_t*)malloc(7 * sizeof(int32_t));
                hash_table_size = 7;
                hash_table_count = 0;
                hash_table_empty_slots = hash_table_size;

                for (long k = 0; k < hash_table_size; k++) {
                    hash_table_slots[k] = EMPTY;
                    hash_table_hashes[k] = -1;
                }
            } else {
                int32_t oldSize = hash_table_size;
                int32_t* oldSlots = hash_table_slots;
                int32_t* oldHashes = hash_table_hashes;

                //make sure the table's not too small
                hash_table_size = computeNextPrime(hash_table_count * 4 + 7);

                hash_table_slots = (int32_t*)malloc(hash_table_size * sizeof(int32_t));
                hash_table_hashes = (int32_t*)malloc(hash_table_size * sizeof(int32_t));

                for (long k = 0; k < hash_table_size; k++) {
                    hash_table_slots[k] = EMPTY;
                    hash_table_hashes[k] = -1;
                }

                hash_table_count = 0;
                hash_table_empty_slots = hash_table_size;

                for (long k = 0; k < oldSize; k++) {
                    if (oldSlots[k] != EMPTY && oldSlots[k] != DELETED) {
                        add(oldHashes[k], oldSlots[k]);
                    }
                }

                free(oldSlots);
                free(oldHashes);
            }
        }

        void prepareForDeserialization(uint32_t slotCount, size_t kv_pair_size) {
            if (hash_table_size) {
                throw std::runtime_error("deserialization prepare should only be called on empty tables");
            }

            items_reserved = slotCount;
            items_populated = (uint8_t*)malloc(slotCount);
            items = (uint8_t*)malloc(slotCount * kv_pair_size);

            for (long k = 0; k < items_reserved; k++) {
                items_populated[k] = true;
            }

            top_item_slot = items_reserved;
        }

        template<class hash_fun_type>
        void buildHashTableAfterDeserialization(size_t kv_pair_size, const hash_fun_type& hash_fun) {
            hash_table_size = computeNextPrime(items_reserved * 2.5 + 7);
            hash_table_slots = (int32_t*)malloc(hash_table_size * sizeof(int32_t));
            hash_table_hashes = (int32_t*)malloc(hash_table_size * sizeof(int32_t));
            hash_table_count = 0;
            hash_table_empty_slots = hash_table_size;

            for (long k = 0; k < hash_table_size; k++) {
                hash_table_slots[k] = EMPTY;
                hash_table_hashes[k] = -1;
            }

            for (long k = 0; k < items_reserved; k++) {
                add(hash_fun(items + kv_pair_size * k), k);
            }
        }

        void checkInvariants(std::string reason) {
            int64_t popCount = 0;
            for (long k = 0; k < items_reserved; k++) {
                if (items_populated[k]) {
                    popCount++;
                    if (top_item_slot <= k) {
                        throw std::runtime_error(reason + ": top item slot should be greater than all populated items");
                    }
                }
            }

            if (popCount != hash_table_count) {
                throw std::runtime_error(reason + ": populated item count is not the same as the hashtable count");
            }

            int64_t filledSlots = 0;
            int64_t deletedSlots = 0;
            for (long k = 0; k < hash_table_size; k++) {
                if (hash_table_slots[k] == DELETED) {
                    deletedSlots++;
                } else if (hash_table_slots[k] != EMPTY) {
                    filledSlots++;

                    if (hash_table_slots[k] >= items_reserved) {
                        throw std::runtime_error(reason + ": hash table has slot entry out of bounds with item list");
                    }

                    if (!items_populated[hash_table_slots[k]]) {
                        throw std::runtime_error(reason + ": hash table points to unmarked slot");
                    }
                }
            }

            if (filledSlots != hash_table_count) {
                throw std::runtime_error(reason + ": Filled slot count is not the same as the hashtable's known count");
            }

            if (hash_table_size - filledSlots - deletedSlots != hash_table_empty_slots) {
                throw std::runtime_error(reason + ": empty slot count is not consistent");
            }
        }

        std::atomic<int64_t> refcount;

        uint8_t* items; //packed set of key_value pairs.
        uint8_t* items_populated; //array of bool for whether populated
        size_t items_reserved; //count of items reserved
        size_t top_item_slot; //index of the next item slot to use

        int32_t* hash_table_slots; //a hashtable. each actual object hash to the slot it holds. -1 if not populated.
        int32_t* hash_table_hashes; //a hashtable. each actual object hash to the slot it holds. -1 if not populated.
        size_t hash_table_size; //size of the table
        size_t hash_table_count; //populated count of the table
        size_t hash_table_empty_slots; //slots that are not empty in the table
    };

public:
    Dict(Type* key, Type* value) :
            Type(TypeCategory::catDict),
            m_key(key),
            m_value(value)
    {
        forwardTypesMayHaveChanged();
    }

    template<class visitor_type>
    void _visitContainedTypes(const visitor_type& visitor) {
    }

    template<class visitor_type>
    void _visitReferencedTypes(const visitor_type& visitor) {
        visitor(m_key);
        visitor(m_value);
    }

    void _forwardTypesMayHaveChanged();

    bool isBinaryCompatibleWithConcrete(Type* other);

    static Dict* Make(Type* key, Type* value);

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
        layout& l = **(layout**)self;

        uint32_t id;
        bool isNew;
        std::tie(id, isNew) = buffer.cachePointer(&l, this);
        buffer.write_uint32(id);

        if (isNew) {
            buffer.write_uint32(l.hash_table_count);

            for (long k = 0; k < l.items_reserved; k++) {
                if (l.items_populated[k]) {
                    m_key->serialize(l.items + m_bytes_per_key_value_pair * k, buffer);
                    m_value->serialize(l.items + m_bytes_per_key_value_pair * k + m_bytes_per_key, buffer);
                }
            }
        }
    }

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
        int32_t id = buffer.read_uint32();

        void* ptr = buffer.lookupCachedPointer(id);

        if (ptr) {
            *((layout**)self) = (layout*)ptr;
            (*(layout**)self)->refcount++;
            return;
        }

        constructor(self);
        layout& l = **((layout**)self);

        //incref it before putting it in.
        l.refcount++;
        buffer.addCachedPointer(id, *((layout**)self), this);

        int32_t count = buffer.read_uint32();

        l.prepareForDeserialization(count, m_bytes_per_key_value_pair);

        for (long k = 0; k < count; k++) {
            m_key->deserialize(l.items + m_bytes_per_key_value_pair * k, buffer);
            m_value->deserialize(l.items + m_bytes_per_key_value_pair * k + m_bytes_per_key, buffer);
        }

        l.buildHashTableAfterDeserialization(
            m_bytes_per_key_value_pair,
            [&](instance_ptr ptr) { return m_key->hash32(ptr); }
            );

        l.checkInvariants("after deserialization");
    }

    void repr(instance_ptr self, ReprAccumulator& stream);

    int32_t hash32(instance_ptr left);

    bool cmp(instance_ptr left, instance_ptr right, int pyComparisonOp);

    int64_t refcount(instance_ptr self) const;

    int64_t size(instance_ptr self) const;

    int64_t slotCount(instance_ptr self) const;

    bool slotPopulated(instance_ptr self, size_t offset) const;

    instance_ptr keyAtSlot(instance_ptr self, size_t offset) const;

    instance_ptr valueAtSlot(instance_ptr self, size_t offset) const;

    instance_ptr lookupValueByKey(instance_ptr self, instance_ptr key) const;

    // insert a new key, copy constructing 'key' but returning an uninitialized value pointer.
    instance_ptr insertKey(instance_ptr self, instance_ptr key) const;

    bool deleteKey(instance_ptr self, instance_ptr key) const;

    void constructor(instance_ptr self);

    void destroy(instance_ptr self);

    void copy_constructor(instance_ptr self, instance_ptr other);

    void assign(instance_ptr self, instance_ptr other);

    Type* keyValuePairType() const { return m_key_value_pair_type; }
    Type* keyType() const { return m_key; }
    Type* valueType() const { return m_value; }

private:
    Type* m_key;
    Type* m_value;
    Type* m_key_value_pair_type;
    size_t m_bytes_per_key;
    size_t m_bytes_per_key_value_pair;
};

