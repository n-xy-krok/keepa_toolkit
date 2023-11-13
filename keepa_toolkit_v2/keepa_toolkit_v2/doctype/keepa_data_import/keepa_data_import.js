// Copyright (c) 2023, N_XY and contributors
// For license information, please see license.txt

frappe.ui.form.on('Keepa Data Import', {
	// refresh: function(frm) {

	// }
	process_file_button: function(frm){
		frm.call('process_exported_files');
		let path = "/app/keepa-analysis-tools"; // ?price_analysis_name=" + cur_frm.docname + '&profit=%5B">"%2C0%5D';
    	console.log(path);
    	frappe.set_route(path);
	}
});
