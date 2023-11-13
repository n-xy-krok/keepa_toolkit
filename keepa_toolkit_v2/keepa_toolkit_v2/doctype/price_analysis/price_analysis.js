
frappe.ui.form.on('Price Analysis', {
	start_processing_button: function(frm){
    	frm.call('start_price_processing');
    	let path = "/app/keepa-analysis-tools"; // ?price_analysis_name=" + cur_frm.docname + '&profit=%5B">"%2C0%5D';
    	console.log(path);
    	frappe.set_route(path);
	}
})