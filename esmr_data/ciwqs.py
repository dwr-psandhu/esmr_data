# California Integrated Water Quality System Project (CIWQS)
# The NPDES permits for discharge points are in this system and parsing the PDF to get them is 
# the only way I have found to get this information

def get_npdes_permit_pdf(placeid):
    url = f'https://ciwqs.waterboards.ca.gov/ciwqs/readOnly/CiwqsReportServlet?inCommand=drilldown&reportName=facilityAtAGlance&placeID={placeid}'
