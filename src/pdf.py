from dataclasses import dataclass

from fpdf import FPDF


def create_pdf_report(
    title: str, 
    output_file_name: str,
    images: dict[str, tuple[int, int]] | None = None,
    font_family: str | None = "Arial",
    font_size: int = 18,
    font_style: str = '',
):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font(font_family, font_style, font_size)
    pdf.cell(0, 10, title, ln=True, align='C')
    
    if images:
        y_cursor = 20
        margin = 10
        for image_path, (width, height) in images.items():
            page_height = pdf.h - pdf.b_margin
            if y_cursor + height > page_height:
                pdf.add_page()
                y_cursor = margin
            pdf.image(
                name=image_path,
                x=margin,
                y=y_cursor,
                w=width,
                h=height
            )
            y_cursor += height + margin
            
    pdf.output(output_file_name)