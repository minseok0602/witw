import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'apps'))
from verify_sources import source_digest,verify


class SourceIntegrityTest(unittest.TestCase):
    def test_line_endings_are_portable_but_content_changes_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);source=root/'kernel.py';audit=root/'audit.json'
            source.write_bytes(b'x = 1\n');audit.write_text(json.dumps({'kernel.py':source_digest(source)}))
            source.write_bytes(b'x = 1\r\n');self.assertEqual(verify(root,audit),1)
            source.write_bytes(b'x = 2\r\n')
            with self.assertRaises(ValueError):verify(root,audit)


if __name__=='__main__':unittest.main()
