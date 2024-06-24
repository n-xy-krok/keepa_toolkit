// Copyright (c) 2023, N_XY and contributors
// For license information, please see license.txt


function get_selected_grid_rows(frm){
	return frm.grids[0].grid.get_selected()
}

function get_data_based_on_fieldtype(df, data, value) {
	let fieldname = df.fieldname;
	let fieldtype = df.fieldtype;
	let fieldvalue = data[fieldname];

	if (fieldtype === "Check") {
		value = frappe.utils.string_to_boolean(value);
		return Boolean(fieldvalue) === value && data;
	} else if (fieldtype === "Sr No" && data.idx.toString().includes(value)) {
		return data;
	} else if (fieldtype === "Duration" && fieldvalue) {
		let formatted_duration = frappe.utils.get_formatted_duration(fieldvalue);

		if (formatted_duration.includes(value)) {
			return data;
		}
	} else if (fieldtype === "Barcode" && fieldvalue) {
		let barcode = fieldvalue.startsWith("<svg")
			? $(fieldvalue).attr("data-barcode-value")
			: fieldvalue;

		if (barcode.toLowerCase().includes(value)) {
			return data;
		}
	} else if (["Datetime", "Date"].includes(fieldtype) && fieldvalue) {
		let user_formatted_date = frappe.datetime.str_to_user(fieldvalue);

		if (user_formatted_date.includes(value)) {
			return data;
		}
	} else if (["Currency", "Float", "Int", "Percent", "Rating"].includes(fieldtype)) {
		let num = fieldvalue || 0;

		if (fieldtype === "Rating") {
			let out_of_rating = parseInt(df.options) || 5;
			num = num * out_of_rating;
		}
		if (fieldname === 'profit') {
			if (parseFloat(value) < parseFloat(fieldvalue)){
				// console.log(parseFloat(value), parseFloat(fieldvalue))
				// console.log(data)
				return data
			}
			return null
		}

		if (num.toString().indexOf(value) > -1) {
			return data;
		}
	} else if (fieldvalue && fieldvalue.toLowerCase().includes(value)) {
		return data;
	}
}

frappe.ui.form.on('Keepa Analysis Result', {
	refresh(frm) {

		var profit_elem = document.getElementsByName

		frm.fields_dict.items.grid.get_filtered_data = () => {
			let thi = frm.fields_dict.items.grid

			let all_data = thi.frm ? thi.frm.doc[thi.df.fieldname] : thi.df.data;
	
			if (!all_data) return;
	
			for (const field in thi.filter) {

				all_data = all_data.filter((data) => {
					let { df, value } = thi.filter[field];
					return get_data_based_on_fieldtype(df, data, value.toLowerCase());
				});
			}
	
			return all_data;
		}
	 
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
			if (get_selected_grid_rows(frm).length <= 0){
				frappe.msgprint("Select more products to proceed. (Checkbox)")
				return
			}
			let d = new frappe.ui.Dialog({
				title: 'Enter details',
				fields: [
					{
						label: 'Supplier',
						fieldname: 'supplier',
						fieldtype: 'Link',
						options: "Supplier"
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
							'supplier': values['supplier'],
							'company': values['company'],
							'warehouse': values['warehouse'],
							'currency': values['currency'],
							'territory': values['territory'],
							'conversion_rates': values['conversion_rates'],
							},
						callback: function(resp) { window.location = '/app/purchase-order/' + resp.message}
					})
					d.hide();
				
				},
				
			});
			
			d.show();
			
			
		}, __('Order'));
		   
		   
	}
   });
