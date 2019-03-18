import object_database.keymapping as keymapping

class ChannelInformation:
    def __init__(self, connIdentity, channel):
        self.connIdentity = connIdentity
        self.subscribedTypes = {}
        self.subscribedIndexKeys = {}
        self.subscribedIds = set()
        self.subscribedSchemas = {}
        self.channel = channel
        self.pendingTransactions = {}
        self.definedSchemas = {}

    def handleTransactionData(self, transaction_guid, key_values, set_adds, set_removes, key_versions, index_versions):
        if transaction_guid not in self.pendingTransactions:
            self.pendingTransactions[transaction_guid] = {
                'writes': {},
                'set_adds': {},
                'set_removes': {},
                'key_versions': set(),
                'index_versions': set()
            }

        self.pendingTransactions[transaction_guid]['writes'].update({k: key_values[k] for k in key_values})
        self.pendingTransactions[transaction_guid]['set_adds'].update({k: set(set_adds[k]) for k in set_adds if set_adds[k]})
        self.pendingTransactions[transaction_guid]['set_removes'].update({k: set(set_removes[k]) for k in set_removes if set_removes[k]})
        self.pendingTransactions[transaction_guid]['key_versions'].update(key_versions)
        self.pendingTransactions[transaction_guid]['index_versions'].update(index_versions)

    def extractTransactionData(self, transaction_guid):
        return self.pendingTransactions.pop(transaction_guid)

class ChannelCollectionHandler:

    def __init__(self):
        self._type_to_connid = {}
        self._index_to_connid = {}
        self._connid_to_channel_info  = {}
        self._id_set = set()

    def handleTransactionData(self, connIdentity, transaction_guid, key_values, set_adds, set_removes, key_versions, index_versions):
        channel_info = self._connid_to_channel_info[connIdentity]
        channel_info.handleTransactionData(transaction_guid, key_values, set_adds, set_removes, key_versions, index_versions)

    def extractTransactionData(self, connIdentity, transaction_guid):
        channel_info = self._connid_to_channel_info[connIdentity]
        return channel_info.extractTransactionData(transaction_guid)

    def getChannelsByIndexKey(self, index_key):
        return self._index_to_connid.get(index_key, set())

    def getChannelsByType(self, typename):
        return self._type_to_connid.get(typename, set())

    def getChannel(self, connIdentity):
        channel_info = self._connid_to_channel_info.get(connIdentity)
        return channel_info.channel

    def getChannels(self, connIdentities):
        channel_set = set()
        for connIdentity in connIdentities:
            channel_set.add(self.getChannel(connIdentity))

        return channel_set

    def getSubscribedIds(self, connIdentity):
        channel_info = self._connid_to_channel_info[connIdentity]
        return channel_info.subscribedIds

    def channelAdded(self, connIdentity, channel):
        self._connid_to_channel_info[connIdentity] = ChannelInformation(connIdentity, channel)
        self.handleChangedIds(connIdentity, {connIdentity}, set())
        self._id_set.add(connIdentity)

    def channelDropped(self, connIdentity):
        channel_info = self._connid_to_channel_info[connIdentity]
        self._id_set.remove(connIdentity)

        for schema_name, typename in channel_info.subscribedTypes:
            self._type_to_connid[schema_name, typename].discard(connIdentity)

        for index_key in channel_info.subscribedIndexKeys:
            self._index_to_connid[index_key].discard(connIdentity)
            if not self._index_to_connid[index_key]:
                del self._index_to_connid[index_key]

        del self._connid_to_channel_info[connIdentity]

        self.handleChangedIds(None, set(), connIdentity)

    def getSchemaDefinition(self, connIdentity, schema):
        channel_info = self._connid_to_channel_info[connIdentity]
        return channel_info.definedSchemas.get(schema)

    def handleSubscription(self, schema, typename, fieldname_and_value, source_connIdentity, isLazy, transaction_num):

        channel_info = self._connid_to_channel_info[source_connIdentity]
        definition = self.getSchemaDefinition(source_connIdentity, schema)

        assert definition is not None, "can't subscribe to a schema we don't know about!"

        assert typename is not None

        assert typename in definition, "Can't subscribe to a type we didn't define in the schema: %s not in %s" % (typename, list(definition))

        typedef = definition[typename]

        if fieldname_and_value is None:
            field, val = " exists", keymapping.index_value_to_hash(True)
        else:
            field, val = fieldname_and_value

        if field == '_identity':
            identities = set([val])
        else:
            identities = self._index_to_connid.get(keymapping.index_key_from_names_encoded(schema, typename, field, val),set())

        if fieldname_and_value is not None:
            # this is an index subscription

            if fieldname_and_value[0] != '_identity':
                index_key = keymapping.index_key_from_names_encoded(schema, typename, fieldname_and_value[0], fieldname_and_value[1])

                self._index_to_connid.setdefault(index_key, set()).add(source_connIdentity)

                self._connid_to_channel_info[source_connIdentity].subscribedIndexKeys[index_key] = -1 if not isLazy else transaction_num
            else:
                # an object's identity cannot change, so we don't need to track our subscription to it
                assert not isLazy
        else:
            # this is a type-subscription
            self._type_to_connid.setdefault((schema, typename), set())
            self._type_to_connid[schema, typename].add(source_connIdentity)

            self._connid_to_channel_info[source_connIdentity].subscribedTypes[(schema, typename)] = -1 if not isLazy else transaction_num



    def findChannelsTransaction(self, key_value, set_adds, set_removes):
        channelsTriggered = set()
        schemaTypePairsWriting = set()
        identities_mentioned = set()

        for subset in [set_adds, set_removes]:
            for k in subset:
                if subset[k]:
                    schema_name, typename = keymapping.split_index_key(k)[:2]

                    schemaTypePairsWriting.add((schema_name, typename))

                    identities_mentioned.update(subset[k])

        for key in key_value:
            schema_name, typename, ident = keymapping.split_data_key(key)[:3]
            schemaTypePairsWriting.add((schema_name, typename))

            identities_mentioned.add(ident)

        print("Called function")
        print(schemaTypePairsWriting)
        print(identities_mentioned)

        for schema_type_pair in schemaTypePairsWriting:
            for channel in self._type_to_connid.get(schema_type_pair, ()):
                channelsTriggered.add(channel)

        for i in identities_mentioned:
            if i in self._id_set:
                channelsTriggered.add(i)
        return channelsTriggered

    def getIncreasedSubscriptions(self, connIdentity, set_adds):
        increased_subscriptions = []

        for add_index, added_identities in set_adds.items():
            schema_name, typename, fieldname, fieldval = keymapping.split_index_key_full(add_index)
            if fieldname == ' exists':
                if (schema_name, typename) not in self._connid_to_channel_info[connIdentity].subscribedTypes:
                    increased_subscriptions.append((add_index, added_identities))

        return increased_subscriptions

    def handleIncreasedSubscriptions(self, connIdentity, increased_subscriptions):
        for add_index, added_identities in increased_subscriptions:
            self._connid_to_channel_info[connIdentity].subscribedIds.update(added_identities)

        return increased_subscription

    def handleChangedIds(self, connIdentity, newIds, oldIds):
        if connIdentity is not None:
            channel_info = self._connid_to_channel_info[connIdentity]
            for new_id in newIds:
                if new_id in self._id_set:
                    channel_info.subscribedIds.add(new_id)

    def handleDefineSchema(self, connIdentity, name, definition):
        channel_info = self._connid_to_channel_info[connIdentity]
        channel_info.definedSchemas[name] = definition

    def handleTransaction(self, key_values, set_adds, set_removes):
        return