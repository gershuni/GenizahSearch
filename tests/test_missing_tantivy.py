import importlib
import sys
from unittest import TestCase, mock


class MissingTantivyImportTest(TestCase):
    def test_genizah_core_reports_missing_tantivy_cleanly(self):
        # sys.modules is put back as it was: later tests hold the genizah_core they imported,
        # and code that imports it again must get that same module, not a fresh one whose
        # CURRENT_LANG those tests never set.
        with mock.patch.dict(sys.modules):
            sys.modules.pop("genizah_core", None)

            with mock.patch.dict(sys.modules, {"tantivy": None}):
                with self.assertRaises(ImportError) as ctx:
                    importlib.import_module("genizah_core")

        self.assertIn("Tantivy library missing", str(ctx.exception))
        self.assertNotIsInstance(ctx.exception, NameError)
