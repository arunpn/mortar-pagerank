/**
 * patents: Pig macros for use in pigscripts.
 */

DEFINE LOAD_PATENTS(input_path)
RETURNS patents {
    $patents = LOAD '$input_path' USING PigStorage() AS (
        publication: (doc_num: chararray, date: chararray, kind: chararray), 
        application: (doc_num: chararray, date: chararray, type: chararray, series_code: chararray), 
        term_extension_days: int, 
        sir_flag: int, 
        invention_title: chararray, 
        stats: (
            claims: int, drawing_sheets: int, figures: int, 
            national_classifications: int, ipcr_classifications: int, foc_classifications: int, 
            citations: int, patent_citations: int, npl_citations: int, related_documents: int, 
            applicants: int, agents: int, assignees: int
        ), 
        classifications: (
            national_main: chararray, national_further: {t: (class: chararray)}, 
            ipcr: {t: (
                version_date: chararray, 
                classification_level: chararray, 
                section: chararray, 
                class: chararray, subclass: chararray, 
                main_group: chararray, subgroup: chararray, 
                symbol_position: chararray, classification_value: chararray, 
                action_date: chararray, generating_office: chararray, 
                classification_status: chararray, classification_data_source: chararray
            )}, 
            field_of_classification_search: (
                national: {t: (class: chararray)}, 
                ipcr: {t: (class: chararray)}
            )
        ), 
        citations: {t: (
            country: chararray, doc_num: chararray, kind: chararray, name: chararray, date: chararray
        )}, 
        parties: (
            applicants: {t: (
                type: chararray, 
                last: chararray, first: chararray, 
                city: chararray, state: chararray, 
                postcode: chararray, country: chararray
            )}, 
            agents: {t: (
                type: chararray, orgname: chararray, 
                last: chararray, first: chararray, 
                city: chararray, state: chararray, 
                postcode: chararray, country: chararray
            )}
        ), 
        assignees: {t: (
            orgname: chararray, role: chararray, 
            city: chararray, state: chararray, 
            postcode: chararray, country: chararray
        )}, 
        examiners: (
            primary_last: chararray, primary_first: chararray, 
            department: chararray, 
            assistant_last: chararray, assistant_first: chararray
        ), 
        abstract: chararray
    );
};