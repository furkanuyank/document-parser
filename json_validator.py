import json
import re

class JSONValidator:
    """JSON çıktılarını doğrulamak için kullanılan sınıf."""
    
    def __init__(self, schema=None):
        """
        JSON doğrulayıcı sınıfını başlatır.
        
        Args:
            schema (dict, optional): Doğrulama için şema.
        """
        self.schema = schema
        self.generated_schema = self._generate_schema(schema) if schema else None
        
    def _generate_schema(self, query):
        """
        Query'den şema oluşturur.
        
        Args:
            query: Sorgu şeması veya '*'.
            
        Returns:
            dict: JSON şeması.
        """
        if query == "*":
            return None  # Genel sorgu için şema gerekmez
            
        # Sorgu bir dizge ise, JSON'a dönüştürmeyi dene
        if isinstance(query, str):
            try:
                query = json.loads(query)
            except json.JSONDecodeError:
                return None
                
        # Şemayı oluştur
        schema = {}
        if isinstance(query, list):
            schema["type"] = "array"
            if query and len(query) > 0:
                schema["items"] = self._infer_schema(query[0])
        elif isinstance(query, dict):
            schema = self._infer_schema(query)
            
        return schema
        
    def _infer_schema(self, obj):
        """
        Nesneden JSON şeması çıkarır.
        
        Args:
            obj: Şeması çıkarılacak nesne.
            
        Returns:
            dict: JSON şeması.
        """
        schema = {"type": "object", "properties": {}, "required": []}
        
        for key, value in obj.items():
            if isinstance(value, dict):
                schema["properties"][key] = self._infer_schema(value)
            elif isinstance(value, list):
                if value and isinstance(value[0], dict):
                    schema["properties"][key] = {
                        "type": "array",
                        "items": self._infer_schema(value[0])
                    }
                else:
                    schema["properties"][key] = {"type": "array"}
            else:
                schema["properties"][key] = self._infer_type(value)
                
            # Zorunlu alanları işaretle
            schema["required"].append(key)
                
        return schema
        
    def _infer_type(self, value):
        """
        Değerin türünü çıkarır.
        
        Args:
            value: Türü belirlenecek değer.
            
        Returns:
            dict: Tür bilgisi.
        """
        if isinstance(value, str):
            return {"type": "string"}
        elif isinstance(value, int):
            return {"type": "integer"}
        elif isinstance(value, float):
            return {"type": "number"}
        elif isinstance(value, bool):
            return {"type": "boolean"}
        elif value is None:
            return {"type": "null"}
        else:
            return {}
            
    def validate_json_against_schema(self, data, schema=None):
        """
        JSON'u şemaya göre doğrular.
        
        Args:
            data: Doğrulanacak veri.
            schema (optional): Kullanılacak şema.
            
        Returns:
            str or None: Hata mesajı veya None.
        """
        if schema is None:
            return None  # Şema yoksa doğrulama yapamazsın
            
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError as e:
                return f"Invalid JSON: {str(e)}"
                
        errors = []
        if schema.get("type") == "array" and isinstance(data, list):
            for item in data:
                item_result = self._validate_object(item, schema.get("items", {}))
                if item_result:
                    errors.append(item_result)
        elif schema.get("type") == "object" and isinstance(data, dict):
            obj_result = self._validate_object(data, schema)
            if obj_result:
                errors.append(obj_result)
        else:
            errors.append(f"Schema type mismatch: expected {schema.get('type')}")
            
        return ", ".join(errors) if errors else None
        
    def _validate_object(self, obj, schema):
        """
        Nesneyi şemaya göre doğrular.
        
        Args:
            obj: Doğrulanacak nesne.
            schema: Şema.
            
        Returns:
            str or None: Hata mesajı veya None.
        """
        errors = []
        
        # Zorunlu alanları kontrol et
        for required_field in schema.get("required", []):
            if required_field not in obj:
                errors.append(f"Missing required field: {required_field}")
                
        # Alanları doğrula
        for key, value in obj.items():
            if key not in schema.get("properties", {}):
                continue  # Şemada olmayan alanları atla
                
            prop_schema = schema["properties"][key]
            expected_type = prop_schema.get("type")
            
            if expected_type == "object" and isinstance(value, dict):
                obj_result = self._validate_object(value, prop_schema)
                if obj_result:
                    errors.append(obj_result)
            elif expected_type == "array" and isinstance(value, list):
                if "items" in prop_schema and value:
                    for item in value:
                        if isinstance(item, dict):
                            item_result = self._validate_object(item, prop_schema["items"])
                            if item_result:
                                errors.append(item_result)
            elif not self._type_matches(value, expected_type):
                errors.append(f"Type mismatch for {key}: expected {expected_type}")
                
        return ", ".join(errors) if errors else None
        
    def _type_matches(self, value, expected_type):
        """
        Değerin beklenen türe uygun olup olmadığını kontrol eder.
        
        Args:
            value: Kontrol edilecek değer.
            expected_type: Beklenen tür.
            
        Returns:
            bool: Tür eşleşirse True.
        """
        if expected_type == "string":
            return isinstance(value, str)
        elif expected_type == "number":
            return isinstance(value, (int, float))
        elif expected_type == "integer":
            return isinstance(value, int)
        elif expected_type == "boolean":
            return isinstance(value, bool)
        elif expected_type == "null":
            return value is None
        elif expected_type == "array":
            return isinstance(value, list)
        elif expected_type == "object":
            return isinstance(value, dict)
        else:
            return True  # Bilinmeyen türler için True döndür