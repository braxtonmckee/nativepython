import time

from object_database.web.cells import Cells


def waitForCellsCondition(cells: Cells, condition, timeout=10.0):
    assert cells.db.serializationContext is not None

    t0 = time.time()
    while time.time() - t0 < timeout:
        condRes = condition()

        if not condRes:
            time.sleep(.1)
            while cells.processOneTask():
                pass
            cells.renderMessages()
        else:
            return condRes

    exceptions = cells.childrenWithExceptions()
    if exceptions:
        raise Exception("\n\n".join([e.childByIndex(0).contents for e in exceptions]))

    return None
