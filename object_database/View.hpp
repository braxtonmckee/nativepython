#pragma once

/**********
Views provide running (native)python threads with a snapshotted view of the current
object-database's subscribed data. A view holds a reference to a collection of objects
that have different representation at different transaction_ids, along with a single
transaction_id. We show a coherent view of all the objects whose versions are <= the
given transaction_id.

We also track all objects and indices we read or write during execution.
***********/

class View {
public:

};