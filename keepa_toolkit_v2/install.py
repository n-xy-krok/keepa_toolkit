import frappe

from loguru import logger


def after_install():
    
    if not frappe.db.exists('Price Analysis', 'CategoryRetrieve'):
        new_doc = frappe.get_doc(
            {
                'doctype': 'Price Analysis',
                'label': 'CategoryRetrieve',
                'status': 'Success'
            }
        )
        new_doc.insert()
        frappe.db.commit()
        logger.info(f'Service doctype of type Price Analysis created with name: CategoryRetrieve')
    
    if not frappe.db.exists('Number Card', 'Keepa Products In Cache'):
        new_doc = frappe.get_doc(
            {
                'doctype': 'Number Card',
                'label': 'Keepa Products In Cache',
                'type': 'Custom',
                'method': 'keepa_toolkit_v2.db_utils.db_helper.get_product_entry_count',
            }
        )
        new_doc.insert()
        frappe.db.commit()
        logger.info(f'Number Card {new_doc.label} created')
        
        
    
    if not frappe.db.exists('Number Card', 'UPC-ASIN Association count'):
        new_doc = frappe.get_doc(
            {
                'doctype': 'Number Card',
                'label': 'UPC-ASIN Association count',
                'type': 'Custom',
                'method': 'keepa_toolkit_v2.db_utils.db_helper.get_association_count',
            }
        )
        new_doc.insert()
        frappe.db.commit()
        logger.info(f'Number Card {new_doc.label} created')
    
    if not frappe.db.exists('Number Card', 'Objects In Queue'):
        new_doc = frappe.get_doc(
            {
                'doctype': 'Number Card',
                'label': 'Objects In Queue',
                'type': 'Document Type',
                'function': 'Count',
                'stats_time_interval': 'Daily',
                'document_type': 'Keepa Retrieving Queue Item Holder'
            }
        )
        new_doc.insert()
        frappe.db.commit()
        logger.info(f'Number Card {new_doc.label} created')
        
    create_table_query_1 = """
        
        CREATE TABLE IF NOT EXISTS upc_asin (
            id BIGINT NOT NULL AUTO_INCREMENT,
            upc VARCHAR(255) NOT NULL,
            asin VARCHAR(12) NOT NULL,
            PRIMARY KEY (id),
            UNIQUE KEY uniq_combination (upc, asin)
        );"""
        
    create_table_query_2 = """
    
    CREATE TABLE IF NOT EXISTS product_compressed (
        id BIGINT NOT NULL AUTO_INCREMENT,
        title VARCHAR(255) DEFAULT NULL,
        asin VARCHAR(255) NOT NULL,
        root_category VARCHAR(255) DEFAULT NULL,
        brand VARCHAR(255) DEFAULT NULL,
        url VARCHAR(255) DEFAULT NULL,
        count_on_amazon SMALLINT DEFAULT 1,
        buy_box_90_days_avg DECIMAL DEFAULT 0,
        new_offer_count_current SMALLINT DEFAULT 0,
        fba_fee DECIMAL DEFAULT 0,
        referral_fee DECIMAL DEFAULT 0,
        package_height FLOAT DEFAULT NULL,
        package_width FLOAT DEFAULT NULL,
        package_length FLOAT DEFAULT NULL,
        package_weight FLOAT DEFAULT NULL,
        sales_rank_current BIGINT DEFAULT 0,
        reviews_rating DECIMAL DEFAULT 0,
        reviews_count INT DEFAULT 0,
        reviews_count_30_days_avg INT DEFAULT 0,
        reviews_count_180_days_avg INT DEFAULT 0,
        review_velocity INT DEFAULT 0,
        availability_of_amazon_offer INT DEFAULT 0,
        variations_count INT DEFAULT 0,
        updated_at DATE DEFAULT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_asin_constraint (asin)
    );

    """
    frappe.db.sql(create_table_query_1)
    frappe.db.sql(create_table_query_2)