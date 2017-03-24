import sys
import os

import mock
from mock import patch
from aip import app
import unittest
from tests.stubdata import stubdata

class TestWorkers(unittest.TestCase):
    
    def xcreate_app(self):
        app.init_app({
            'SQLALCHEMY_URL': 'sqlite:///',
            'SQLALCHEMY_ECHO': False
        })
        Base.metadata.bind = app.session.get_bind()
        Base.metadata.create_all()
        return app
    
    def xtearDown(self):
        test_base.TestUnit.tearDown(self)
        Base.metadata.drop_all()
        app.close_app()
        
    
    @patch('aip.app.task_merge_metadata.delay', return_value=None)
    @patch('aip.libs.read_records.readRecordsFromADSExports', return_value=[stubdata.INPUT_DOC])
    def test_task_merge_metadata(self, task_merge_metadata, *args):
        self.assertFalse(task_merge_metadata.called)
        app.task_read_records([('bibcode', 'fingerprint')])
        self.assertTrue(task_merge_metadata.called)
        
    
    @patch('aip.app.task_update_record.delay', return_value=None)
    def test_task_update_record(self, task_update_record, *args):
        self.assertFalse(task_update_record.delay.called)
        app.task_merge_metadata(stubdata.INPUT_DOC)
        self.assertTrue(task_update_record.delay.called)