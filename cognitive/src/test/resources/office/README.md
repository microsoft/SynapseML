# Synthetic Office invoices

These are real legacy Office binary files and OOXML packages containing fictional
invoices, not renamed ZIP stubs. They contain no customer data, macros, or external
data connections.

| File | Layout | Invoice | Grand total (USD) |
| --- | --- | --- | --- |
| `invoice.doc`, `invoice.docx` | Word invoice with a line-item table and payment terms | INV-1042 | 246.90 |
| `invoice.xls`, `invoice.xlsx` | Invoice overview and separate line-items worksheet | INV-2084 | 412.75 |
| `invoice.ppt`, `invoice.pptx` | Three slides: invoice header, services, payment summary | INV-3096 | 835.20 |

The fictional supplier is Fabrikam Demo Office and the customer is Contoso Demo
Labs. The files were generated with python-docx, openpyxl, and python-pptx,
respectively, with synthetic document metadata and fixed ZIP timestamps.
LibreOffice's Word/Excel/PowerPoint 97 export filters produced the genuine binary
DOC/XLS/PPT counterparts. Binary output may vary with the converter version.
Tests read the checked-in bytes without requiring these libraries or an Office installation.
