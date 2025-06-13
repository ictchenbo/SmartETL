"""
基于docling库进行pdf解析
参考：https://docling-project.github.io/docling/examples/export_figures/
"""
import io
import time

__start = time.time()

try:
    from docling.datamodel.base_models import InputFormat
    from docling.pipeline.simple_pipeline import SimplePipeline
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption, WordFormatOption
    from docling_core.types.io import DocumentStream
    from docling_core.types.doc import ImageRefMode, PictureItem, TableItem, TextItem
except:
    print("docling required: pip install dockling")
    raise ModuleNotFoundError("docling not installed")

converter_no_image = DocumentConverter()

pipeline_options = PdfPipelineOptions()
pipeline_options.images_scale = 2.0
pipeline_options.generate_picture_images = True

converter_with_image = DocumentConverter(
    format_options={
        InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
    }
)

converter_word = DocumentConverter(
    format_options={
        InputFormat.DOCX: WordFormatOption(pipeline_cls=SimplePipeline)  # , backend=MsWordDocumentBackend
    }
)

__end = time.time()
print(f'docling DocumentConverter init using {__end - __start} seconds')


def extract_pdf(row: dict,
                data_key: str = 'data',
                md_key: str = 'md',
                image_key: str = 'images'):
    """解析PDF文件提取文字"""
    converter = converter_with_image if image_key else converter_no_image

    data = row[data_key]
    if isinstance(data, bytes):
        result = converter.convert(DocumentStream(name="doc.pdf", stream=io.BytesIO(data)))
    else:
        result = converter.convert(data)

    doc = result.document

    row[md_key] = doc.export_to_markdown()

    if image_key:
        images = []
        for element, _level in doc.iterate_items():
            if isinstance(element, PictureItem):
                image_buffer = io.BytesIO()
                element.get_image(doc).save(image_buffer, "PNG")
                images.append(image_buffer.getvalue())
        row[image_key] = images

    return row


def extract_word(data: str or bytes):
    """解析Word文件提取文字内容 生成markdown格式"""
    if isinstance(data, bytes):
        result = converter_word.convert(DocumentStream(name="doc.docx", stream=io.BytesIO(data)))
    else:
        result = converter_word.convert(data)

    doc = result.document

    return doc.export_to_markdown()
