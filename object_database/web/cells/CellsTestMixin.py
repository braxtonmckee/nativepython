from object_database.web.cells import Cells
from object_database.web.cells.util import waitForCellsCondition


class CellsTestMixin:
    @staticmethod
    def waitForCells(cells: Cells, condition, timeout=10.0):
        return waitForCellsCondition(cells, condition, timeout)

    def assertNoCellExceptions(self, cells: Cells):
        exceptions = cells.childrenWithExceptions()
        self.assertTrue(not exceptions, "\n\n".join([e.childByIndex(0).contents for e in exceptions]))

    def assertCellTagExists(self, cells: Cells, tag: str, expected_count=1):
        res = self.waitForCells(
            cells,
            lambda: cells.findChildrenByTag(tag)
        )
        self.assertIsNotNone(res)
        self.assertEqual(len(res), expected_count)

    def assertCellTypeExists(self, cells: Cells, typ, expected_count=1):
        res = self.waitForCells(
            cells,
            lambda: cells.findChildrenMatching(lambda cell: isinstance(cell, typ))
        )
        self.assertIsNotNone(res)
        self.assertEqual(len(res), expected_count)

    def check_ui_script(self, cells: Cells, script, root_cell=None):
        """ Perform a sequence of actions on the UI and check the results

        Parameters
        ----------
        cells: Cells
            the Cells object against which to perform the script

        script: list of steps
            the list of steps to be performed. The syntax of the steps
            is described below

        root_cell: Cell
            the root cell from which to start the script; normally this
            is None and we execute the script on the cells root, but useful
            for recursive scripts

        Step Syntax
        -----------
        Each step is a dictionary. The keys of the dictionary determine the
        types of actions that will be taken, and the values determine the
        specific actions. Each step can be divided in the selection phase
        and the execution phase. In the selection phase, we select the set
        of cells that we will be operating on, typically a single cell, and
        in the execution phase we perform actions and checks on the selected
        cells.

        * Step Keys
        -----------
        * A. Selection Phase
        --------------------
        tag: str
            when this key is present, we start by selecting the cells that have
            a matching tag

        exp_cnt: int
            the number of cells we expect to find; defaults to 1 when undefined

        cond: code:str or tuple(code:str, vars_in_scope:dict(name:str, var:obj))
            an expression to check for truthiness and the variables needed to
            evaluate it. The cells selected in this phase are checked against
            this condition to decide whether they are ultimately selected or
            not. The variable `cell` can be used in the condition expession to
            refer to each cell under test.

        * B. Execution Phase
        --------------------
        msg: dictionary
            when present, call `onMessageWithTransaction` with its value on
            all matching cells

        check: code:str or tuple(code:str, vars_in_scope:dict(name:str, var:obj))
            an expression to check for truthiness and the variables needed to
            evaluate it. The expression is evaluated against each cell selected
            in the previous phase and will usually contain a reference to the
            variable `cell` which will be defined by the script execution engine.

        script: list of steps
            a script to be executed recursively on the selected cells. This
            capability comes in handy when we want to interact with elements
            of modal windows which appear as we click on buttons.

        """

        def codeAndVars(codeAndMaybeVars, cell):
            if isinstance(codeAndMaybeVars, str):
                # just code; no vars
                code = codeAndMaybeVars
                vars_in_ctx = {}
            else:
                # code and vars
                code = codeAndMaybeVars[0]
                vars_in_ctx = codeAndMaybeVars[1]

            vars_in_ctx['cell'] = cell

            return code, vars_in_ctx

        root_cell = root_cell or cells
        for step in script:
            selected_cells = []

            if 'tag' in step:
                selected_cells = root_cell.findChildrenByTag(step['tag'])

            if 'cond' in step:
                really_selected_cells = []
                for cell in selected_cells:
                    code, vars_in_ctx = codeAndVars(step['cond'], cell)
                    res = eval(compile(code, "<string>", "eval"), vars_in_ctx)
                    if res:
                        really_selected_cells.append(cell)

                selected_cells = really_selected_cells

            expected_count = self.expected_count_for_step(step)

            if expected_count is not None:
                self.assertEqual(
                    len(selected_cells), expected_count,
                    f"{len(selected_cells)} != {expected_count} for cells with tag '{step['tag']}'"
                )

            for cell in selected_cells:
                if 'msg' in step:
                    cell.onMessageWithTransaction(step['msg'])
                    cells.renderMessages()

                if 'check' in step:
                    code, vars_in_ctx = codeAndVars(step['check'], cell)

                    res = eval(compile(code, "<string>", "eval"), vars_in_ctx)
                    self.assertTrue(
                        res,
                        f"'{code}' is not True"
                    )

                if 'script' in step:
                    self.check_ui_script(cells, step['script'], root_cell=cell)

    def expected_count_for_step(self, step):
        if 'exp_cnt' in step:
            return step['exp_cnt']

        if 'cond' in step:
            return None

        return 1
