from pyspark.sql.functions import udf, lit, col
from pyspark.sql.types import StringType
from PIL import Image
import io
import codecs

# Define a UDF para redimensionar as imagens
def resize_image(data, width, height):
    image = Image.open(io.BytesIO(data))
    resized_image = image.resize((width, height))
    byte_array = io.BytesIO()
    resized_image.save(byte_array, format='PNG')
    return codecs.decode(codecs.encode(byte_array.getvalue(), "base64"), 'utf-8')

# Registra a UDF
resize_image_udf = udf(resize_image, StringType())