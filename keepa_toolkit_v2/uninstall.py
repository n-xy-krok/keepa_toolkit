import frappe

from loguru import logger


def after_uninstall():
    if frappe.db.exists('Number Card', 'Keepa Products In Cache'):
        frappe.delete_doc('Number Card', 'Keepa Products In Cache')

    if frappe.db.exists('Number Card', 'UPC-ASIN Association count'):
        frappe.delete_doc('Number Card', 'UPC-ASIN Association count')
    
    if frappe.db.exists('Number Card', 'Objects In Queue'):
        frappe.delete_doc('Number Card', 'Objects In Queue')
    
    frappe.db.commit()
    logger.info('The related "Number Cards" has been removed.')
    
    create_table_query_1 = """
        
        DROP TABLE IF EXISTS upc_asin ;
    
    """
        
    create_table_query_2 = """
    
        DROP TABLE IF EXISTS product_compressed ;

    """
    frappe.db.sql(create_table_query_1)
    frappe.db.sql(create_table_query_2)