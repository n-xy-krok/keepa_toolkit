import frappe
from unittest.mock import Mock

from keepa_toolkit_v2.command.bsr_fetch_command import FetchBSRCommand

from frappe.tests.utils import FrappeTestCase


    
class TestBSRFetch(FrappeTestCase):
    def setUp(self) -> None:
        
        doc = frappe.get_doc({
            "doctype": "Price Analysis",
            "analysis_name": self.test_analysis_name
        }).insert()
        return super().setUp()
    
    def tearDown(self) -> None:
        
        frappe.delete_doc(doctype='Keepa Retrieving Queue Item Holder', name=f"{self.test_analysis_name}-0")
        frappe.delete_doc(doctype='Price Analysis', name=self.test_analysis_name)
        frappe.db.commit()
        
        return super().tearDown()
    
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self.test_analysis_name = "Test Price Analysis"
        
        self.keepa_mock_object = Mock()
        self.keepa_mock_object.best_sellers_query.return_value = ["TESTASIN1", "TESTASIN2", "TESTASIN3"]
        self.keepa_mock_object.tokens_left = 400
        
        self.saving_strategy_mock = Mock()
        self.saving_strategy_mock.save_list.return_value = "saved_to_file"
        
    def test_simple_case(self):
        command = FetchBSRCommand(
            keepa_object=self.keepa_mock_object,
            category_ids=[123, 456],
            saving_strategy=self.saving_strategy_mock,
            rank_limit=100,
            queue_name=self.test_analysis_name
        )
        command.execute()
        
        self.saving_strategy_mock.save_list.assert_called()
        self.keepa_mock_object.best_sellers_query.assert_called()
        
        docs = frappe.get_all(doctype='Keepa Retrieving Queue Item Holder', filters={"name": ['like', f'%{self.test_analysis_name}%']})
        
        self.assertGreater(len(docs), 0)

        