import sys
import os

from collections import OrderedDict
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
    @patch('aip.libs.read_records.readRecordsFromADSExports', 
           return_value=[stubdata.ADSRECORDS['2015ApJ...815..133S'], stubdata.ADSRECORDS['2015ASPC..495..401C']])
    def test_task_read_records(self, next_task, *args):
        self.assertFalse(next_task.called)
        app.task_read_records([('bibcode', 'fingerprint')])
        self.assertTrue(next_task.called)
    
    
    @patch('aip.app.task_update_record.delay', return_value=None)
    def test_task_merge_metadata(self, next_task, *args):
        self.assertFalse(next_task.called)
        app.task_merge_metadata(stubdata.ADSRECORDS['2015ApJ...815..133S'])
        self.assertTrue(next_task.called)
        next_task.assert_called_with(u'2015ApJ...815..133S', u'metadata', stubdata.MERGEDRECS['2015ApJ...815..133S'])
        
    
    @patch('aip.app.task_update_solr.delay', return_value=None)
    @patch('aip.libs.update_records.update_storage', return_value=None)
    def test_task_update_record(self, next_task, update_storage, *args):
        self.assertFalse(next_task.called)
        app.task_update_record('2015ApJ...815..133S', 'metadata', stubdata.MERGEDRECS['2015ApJ...815..133S'])
        self.assertTrue(next_task.called)
        self.assertTrue(update_storage.called)
