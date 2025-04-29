from string import Template

TEMPLATES = {
    "extract_fields": Template(
        """You are a document analysis assistant. Analyze the image and extract the following information:
$fields

Return the information in valid JSON format with the specified fields. Use null for missing or unclear fields.
"""
    ),
    
    "extract_tables": Template(
        """Analyze the table shown in the image. Extract all data from the table and return it as a structured JSON array.
Column headers should be used as keys. Maintain the original structure of the data.

Table data:
"""
    ),
    
    "general_extraction": Template(
        """You are a document analysis assistant. Analyze the document and extract all relevant information.
Return your analysis as a structured JSON with appropriate fields and values.
Use null for missing or unclear information.
"""
    )
}

def get_prompt_template(template_name="general_extraction", **kwargs):
    """
    Belirli bir şablonu ve parametrelerini kullanarak prompt oluşturur.
    
    Args:
        template_name (str): Kullanılacak şablonun adı.
        **kwargs: Şablona eklenecek değişkenler.
        
    Returns:
        str: Doldurulmuş şablon.
    """
    if template_name not in TEMPLATES:
        return TEMPLATES["general_extraction"].safe_substitute(**kwargs)
    
    return TEMPLATES[template_name].safe_substitute(**kwargs)