# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Small synthetic PDF and DOCX inputs, generated without document libraries."""

from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


DOCX_MIME_TYPE = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)
DOCX_FIRST_PAGE = "SYNAPSEML DOCX FIRST PAGE"
DOCX_SECOND_PAGE = "SYNAPSEML DOCX SECOND PAGE"


def synthetic_pdf(page_count=4):
    if page_count < 1:
        raise ValueError("page_count must be positive")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    kids = []
    for number in range(1, page_count + 1):
        page_id = len(objects) + 1
        content_id = page_id + 1
        kids.append(f"{page_id} 0 R")
        objects.append(
            (
                "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_id} 0 R >>"
            ).encode("ascii")
        )
        text = (
            f"BT /F1 16 Tf 50 730 Td (SYNTHETIC INVOICE CU-{number:03d}) Tj "
            "0 -35 Td (Example Company - synthetic test data only) Tj "
            f"0 -35 Td (Page {number} of {page_count}) Tj "
            "0 -35 Td (Invoice date: 2026-09-04) Tj "
            f"0 -35 Td (Widgets: {number} x 10.00 USD) Tj "
            f"0 -35 Td (Total due: {number * 10}.00 USD) Tj ET"
        ).encode("ascii")
        objects.append(
            f"<< /Length {len(text)} >>\nstream\n".encode("ascii")
            + text
            + b"\nendstream"
        )
    objects[1] = (
        f"<< /Type /Pages /Kids [{' '.join(kids)}] /Count {page_count} >>"
    ).encode("ascii")
    data = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, 1):
        offsets.append(len(data))
        data.extend(f"{index} 0 obj\n".encode("ascii") + obj + b"\nendobj\n")
    xref = len(data)
    data.extend(f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode("ascii"))
    for offset in offsets[1:]:
        data.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    data.extend(
        (
            f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\n"
            f"startxref\n{xref}\n%%EOF\n"
        ).encode("ascii")
    )
    return bytes(data)


def synthetic_docx():
    content_types = """<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml"
    ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>"""
    relationships = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1"
    Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"
    Target="word/document.xml"/>
</Relationships>"""
    document = f"""<?xml version="1.0" encoding="UTF-8"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p><w:r><w:t>{DOCX_FIRST_PAGE}</w:t></w:r></w:p>
    <w:p><w:r><w:t>Synthetic receipt CU-DOCX-001</w:t></w:r></w:p>
    <w:tbl>
      <w:tblPr><w:tblW w:w="9000" w:type="dxa"/></w:tblPr>
      <w:tblGrid><w:gridCol w:w="4500"/><w:gridCol w:w="4500"/></w:tblGrid>
      <w:tr>
        <w:tc><w:p><w:r><w:t>Item</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>Amount</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>Synthetic widgets</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>42.00 USD</w:t></w:r></w:p></w:tc>
      </w:tr>
    </w:tbl>
    <w:p><w:r><w:br w:type="page"/></w:r></w:p>
    <w:p><w:r><w:t>{DOCX_SECOND_PAGE}</w:t></w:r></w:p>
    <w:p><w:r><w:t>Synthetic delivery reference CU-DOCX-002</w:t></w:r></w:p>
    <w:sectPr>
      <w:pgSz w:w="12240" w:h="15840"/>
      <w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440"/>
    </w:sectPr>
  </w:body>
</w:document>"""
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        for name, text in (
            ("[Content_Types].xml", content_types),
            ("_rels/.rels", relationships),
            ("word/document.xml", document),
        ):
            # Stable timestamps keep the document request fingerprint identical on resume.
            entry = ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            entry.compress_type = ZIP_DEFLATED
            archive.writestr(entry, text.encode("utf-8"))
    return output.getvalue()
