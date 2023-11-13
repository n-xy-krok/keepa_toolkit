// Copyright (c) 2023, N_XY and contributors
// For license information, please see license.txt


function get_selected_grid_rows(frm){
	return frm.grids[0].grid.get_selected()
}

frappe.ui.form.on('Keepa Analysis Result', {
	refresh(frm) {
	 
		frm.add_custom_button(__('Export to csv'), function(){
			frappe.call({
				'method': 'keepa_toolkit_v2.keepa_toolkit_v2.doctype.keepa_analysis_result.keepa_analysis_result.export_to_csv',
				'args': {'name': frm.doc.name, 'selected_rows': get_selected_grid_rows(frm)},
				callback: function(resp) { window.location = '/app/file?file_name=' + resp.message }
			})
		}, __('Export'));
		frm.add_custom_button(__('Export to xlsx'), function(){
			frappe.call({
				'method': 'keepa_toolkit_v2.keepa_toolkit_v2.doctype.keepa_analysis_result.keepa_analysis_result.export_to_xlsx',
				'args': {'name': frm.doc.name, 'selected_rows': get_selected_grid_rows(frm)},
				callback: function(resp) { window.location = '/app/file?file_name=' + resp.message }
			})
		}, __('Export'));

		let q_args = {
			filters: { company: 'Med Care'}
		}
		frm.add_custom_button(__('Create order'), function(){
			let d = new frappe.ui.Dialog({
				title: 'Enter details',
				fields: [
					{
						label: 'Customer',
						fieldname: 'customer',
						fieldtype: 'Link',
						options: "Customer",
						default: 'aaa'
					},
					{
						label: 'Company',
						fieldname: 'company',
						fieldtype: 'Link',
						options: "Company",
						onchange(){
							console.log(this)
							this.layout.fields_dict.warehouse.df.filters.company = this.value
						}
					},
					{
						label: 'Warehouse',
						fieldname: 'warehouse',
						fieldtype: 'Link',
						options: "Warehouse",
						filters: { company: ''},
						onchange(){

						}
					},
					{
						label: 'Currency',
						fieldname: 'currency',
						fieldtype: 'Link',
						options: "Currency",
						default: "USD"
					},
					{
						label: 'Territory',
						fieldname: 'territory',
						fieldtype: 'Link',
						options: "Territory",
						default: 'Ukraine'
					},
					{
						label: 'Conversion Rates',
						fieldname: 'conversion_rates',
						fieldtype: 'Float',
						default: 1
					},

				],
				size: 'small', // small, large, extra-large 
				primary_action_label: 'Submit',
				primary_action(values) {
					console.log(values)
					frappe.call({
						'method': 'keepa_toolkit_v2.keepa_toolkit_v2.doctype.keepa_analysis_result.keepa_analysis_result.create_order_from_selected',
						'args': {
							'name': frm.doc.name,
							'selected_rows': get_selected_grid_rows(frm),
							'customer': values['customer'],
							'company': values['company'],
							'warehouse': values['warehouse'],
							'currency': values['currency'],
							'territory': values['territory'],
							'conversion_rates': values['conversion_rates'],
							},
						callback: function(resp) { window.location = '/app/sales-order/' + resp.message}
					})
					d.hide();
				
				},
				
			});
			
			d.show();
			
			
		}, __('Order'));
		   
		   
	}
   });
