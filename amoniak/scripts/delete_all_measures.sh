#!/bin/bash
curl --insecure -E ${EMPOWERING_CERT_FILE} -H "\"X-CompanyId: ${EMPOWERING_COMPANY_ID}\"" -X DELETE  https://api.empowering.cimne.com/v1/amon_measures_measurements
